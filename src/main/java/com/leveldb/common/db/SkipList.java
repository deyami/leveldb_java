package com.leveldb.common.db;

import com.leveldb.common.AtomicPointer;
import com.leveldb.common._Comparable;

import java.util.*;

class Node<Key> {
    @SuppressWarnings("unchecked")
    Node(Key k, int iMaxHeight) {
        key = k;
        next_ = new AtomicPointer[iMaxHeight];
        for (int i = 0; i < iMaxHeight; i++) {
            next_[i] = new AtomicPointer<Node<Key>>(null);
        }
    }

    Key key;

    // Accessors/mutators for links. Wrapped in methods so we can
    // add the appropriate barriers as necessary.
    Node<Key> next(int n) {
        assert (n >= 0);
        // Use an 'acquire load' so that we observe a fully initialized
        // version of the returned Node.
        return next_[n].acquireLoad();
    }

    void setNext(int n, Node<Key> x) {
        assert (n >= 0);
        // Use a 'release store' so that anybody who reads through this
        // pointer observes a fully initialized version of the inserted
        // node.
        next_[n].releaseStore(x);
    }

    // No-barrier variants that can be safely used in a few locations.
    Node<Key> noBarrierNext(int n) {
        assert (n >= 0);
        return next_[n].noBarrierLoad();
    }

    void noBarrierSetNext(int n, Node<Key> x) {
        assert (n >= 0);
        next_[n].noBarrierStore(x);
    }

    public String toString() {
        String s = "[" + key.toString() + "]";
        int idx = 0;
        for (AtomicPointer<Node<Key>> n : next_) {
            if (n.acquireLoad() != null) {
                s += (idx + ": " + n.acquireLoad().toString());
            }
            idx += 1;
        }
        return s;
    }

    // Array of length equal to the node height. next_[0] is lowest level
    // link.
    AtomicPointer<Node<Key>> next_[];

}

/**
 * donot need to extend Common.Iterator
 *
 * @param <Key>
 * @param <Comparator>
 * @author wlu
 */
class SkipListIterator<Key, Comparator extends _Comparable<Key>> {
    // Initialize an iterator over the specified list.
    // The returned iterator is not valid.
    public SkipListIterator(SkipList<Key, Comparator> list) {
        list_ = list;
        node_ = null;
    }

    // Returns true iff the iterator is positioned at a valid node.
    boolean valid() {
        return node_ != null;
    }

    // Returns the key at the current position.
    // REQUIRES: valid()
    Key key() {
        assert (valid());
        if (node_ == null) {
            @SuppressWarnings("unused")
            int d = 0;
        }
        return node_.key;
    }

    // Advances to the next position.
    // REQUIRES: valid()
    void next() {
        assert (valid());
        node_ = node_.next(0);
    }

    // Advances to the previous position.
    // REQUIRES: valid()
    void prev() {
        // Instead of using explicit "prev" links, we just search for the
        // last node that falls before key.
        assert (valid());
        node_ = list_.findLessThan(node_.key);
        if (node_ == list_.head_) {
            node_ = null;
        }
    }

    // Advance to the first entry with a key >= target
    void seek(Key target) {
        node_ = list_.findGreaterOrEqual(target, null);
    }

    // Position at the first entry in list.
    // Final state of iterator is valid() iff list is not empty.
    void seekToFirst() {
        node_ = list_.head_.next(0);
    }

    // Position at the last entry in list.
    // Final state of iterator is valid() iff list is not empty.
    void seekToLast() {
        node_ = list_.findLast();
        if (node_ == list_.head_) {
            node_ = null;
        }
    }

    SkipList<Key, Comparator> list_;
    Node<Key> node_;
    // Intentionally copyable
}

public class SkipList<Key, Comparator extends _Comparable<Key>> {

    // parameters
    private final int kMaxHeight = 12;

    // Immutable after construction
    // Arena* const arena_; // Arena used for allocations of nodes

    Node<Key> head_;

    // Modified only by insert(). Read racily by readers, but stale
    // values are ok.
    AtomicPointer<Integer> max_height_; // Height of the entire list

    Comparator compare_;

    private int getMaxHeight() {
        return (max_height_.noBarrierLoad());
    }

    public Node<Key> findLast() {
        Node<Key> x = head_;
        int level = getMaxHeight() - 1;
        while (true) {
            Node<Key> next = x.next(level);
            if (next == null) {
                if (level == 0) {
                    return x;
                } else {
                    // Switch to next list
                    level--;
                }
            } else {
                x = next;
            }
        }
    }

    // Read/written only by insert().
    Random rnd_;

    /* construction */
    public SkipList(Comparator icomparator) {
        compare_ = icomparator;
        head_ = new Node<Key>(null /* any key will do */, kMaxHeight); // TODO
        // need
        // more
        // consideration
        max_height_ = new AtomicPointer<Integer>(1);
        rnd_ = new Random();
        for (int i = 0; i < kMaxHeight; i++) {
            head_.setNext(i, null);
        }
    }

    // find the node that is less than key
    public Node<Key> findLessThan(Key key) {
        Node<Key> x = head_;
        int level = getMaxHeight() - 1;
        while (true) {
            assert (x == head_ || compare_.compare(x.key, key) < 0);
            Node<Key> next = x.next(level);
            if (next == null || compare_.compare(next.key, key) >= 0) {
                if (level == 0) {
                    return x;
                } else {
                    // Switch to next list
                    level--;
                }
            } else {
                x = next;
            }
        }
    }

    /* insert a key into the list */
    public void insert(Key key) {
        List<Node<Key>> prev = new ArrayList<Node<Key>>(kMaxHeight);
        for (int i = 0; i < kMaxHeight; i++) {
            prev.add(new Node<Key>(null, kMaxHeight));
        }
        Node<Key> x = findGreaterOrEqual(key, prev);
        assert (x == null || compare_.compare(key, x.key) != 0);
        int height = randomHeight();
        if (height > getMaxHeight()) {
            for (int i = getMaxHeight(); i < height; i++) {
                prev.set(i, head_);
            }
            // It is ok to mutate max_height_ without any synchronization
            // with concurrent readers. A concurrent reader that observes
            // the new value of max_height_ will see either the old value of
            // new level pointers from head_ (NULL), or a new value set in
            // the loop below. In the former case the reader will
            // immediately drop to the next level since NULL sorts after all
            // keys. In the latter case the reader will use the new node.
            max_height_.noBarrierStore(height);
        }

        x = new Node<Key>(key, height);
        for (int i = 0; i < height; i++) {
            // noBarrierSetNext() suffices since we will add a barrier when
            // we publish a pointer to "x" in prev[i].
            x.noBarrierSetNext(i, prev.get(i).noBarrierNext(i));
            prev.get(i).setNext(i, x);
        }

    }

    // whether key is contained in the list
    public boolean contains(Key key) {
        Node<Key> x = findGreaterOrEqual(key, null);
        if (x != null && compare_.compare(key, x.key) == 0) {
            return true;
        } else {
            return false;
        }
    }

    // randomly generate height for a node
    private int randomHeight() {
        // increase length by i with probability (0.25)^(i-1) * (0.75)
        int kBranching = 4;
        int height = 1;
        while (height < kMaxHeight
                && ((Math.abs(rnd_.nextInt()) % kBranching) == 0)) {
            height++;
        }
        assert (height > 0);
        assert (height <= kMaxHeight);
        return height;
    }

    // find and return the node whose key is >= input key;
    // set prev[0] to be the node right before the returned one (
    // to be as the node's previous). it is interesting to find the
    // return statement is ...
    public Node<Key> findGreaterOrEqual(Key key, List<Node<Key>> prev) {
        Node<Key> x = head_;
        int level = getMaxHeight() - 1;
        while (true) {
            Node<Key> next = x.next(level);
            if (keyIsAfterNode(key, next)) {
                // Keep searching in this list
                x = next;
            } else {
                if (level == 0) {
                    if (prev != null)
                        prev.set(level, x);
                    return next;
                } else {
                    // Switch to next list
                    level--;
                }
            }
        }
    }

    private boolean keyIsAfterNode(Key key, Node<Key> n) {
        // NULL n is considered infinite
        return (n != null) && compare_.compare(n.key, key) < 0;
    }

    boolean Equal(Key a, Key b) {
        return compare_.compare(a, b) == 0;
    }

    // /////////////////////////////////////////////////////////
    // / test cases
    static class TestComparator extends _Comparable<Integer> {

        @Override
        public int compare(Integer a, Integer b) {
            if (a < b) {
                return -1;
            } else if (a > b) {
                return +1;
            } else {
                return 0;
            }
        }
    }

    static class TestInternalKeyComparator extends _Comparable<InternalKey> {

        @Override
        public int compare(InternalKey k, InternalKey k2) {
            // TODO Auto-generated method stub
            return 0;
        }

    }

    static class SkiplistTest {
        void ASSERT_TRUE(boolean b) {
            if (!b) {
                System.out.println(b);
            }
        }

        void ASSERT_EQ(int a, int b) {
            if (a != b) {
                System.out.println(a == b);
            }
        }

        void Empty() {
            TestComparator cmp = new TestComparator();
            SkipList<Integer, TestComparator> list = new SkipList<Integer, TestComparator>(
                    cmp);
            ASSERT_TRUE(!list.contains(10));

            SkipListIterator<Integer, TestComparator> iter = new SkipListIterator<Integer, TestComparator>(
                    list);
            ASSERT_TRUE(!iter.valid());
            iter.seekToFirst();
            ASSERT_TRUE(!iter.valid());
            iter.seek(100);
            ASSERT_TRUE(!iter.valid());
            iter.seekToLast();
            ASSERT_TRUE(!iter.valid());
        }

        void insertAndLookup() {

            class re_integer implements Comparable<re_integer> {
                int val;

                public re_integer(int v) {
                    val = v;
                }

                @Override
                public int compareTo(re_integer arg0) {
                    return arg0.val - val;
                }

            }
            int N = 2000;
            int R = 5000;
            Random rnd = new Random();
            // baseline
            SortedSet<Integer> keys = new TreeSet<Integer>();
            SortedSet<re_integer> keys_reverse = new TreeSet<re_integer>();
            TestComparator cmp = new TestComparator();
            SkipList<Integer, TestComparator> list = new SkipList<Integer, SkipList.TestComparator>(
                    cmp);
            for (int i = 0; i < N; i++) {
                int key = rnd.nextInt() % R;
                if (keys.add(key)) {
                    keys_reverse.add(new re_integer(key));
                    // System.out.println(key);
                    list.insert(key);
                }
            }

            for (int i = 0; i < R; i++) {
                if (list.contains(i)) {
                    ASSERT_TRUE(keys.contains(i));
                } else {
                    ASSERT_TRUE(!keys.contains(i));
                }
            }

            // Simple iterator tests
            {
                SkipListIterator<Integer, TestComparator> iter = new SkipListIterator<Integer, SkipList.TestComparator>(
                        list);
                ASSERT_TRUE(!iter.valid());

                iter.seek(Integer.MIN_VALUE);// different from cpp, 'cause java
                // doesnot support uint64
                ASSERT_TRUE(iter.valid());
                ASSERT_EQ(keys.first(), iter.key());

                iter.seekToFirst();
                ASSERT_TRUE(iter.valid());
                ASSERT_EQ(keys.first(), iter.key());

                iter.seekToLast();
                ASSERT_TRUE(iter.valid());
                ASSERT_EQ(keys.last(), iter.key());
            }

            // Forward iteration test
            for (int i = 0; i < R; i++) {
                SkipListIterator<Integer, TestComparator> iter = new SkipListIterator<Integer, SkipList.TestComparator>(
                        list);
                iter.seek(i);

                // Compare against model iterator
                // std::set<Key>::iterator model_iter = keys.lower_bound(i);
                SortedSet<Integer> model_ = keys.tailSet(i);
                Iterator<Integer> model_iter = model_.iterator();
                for (int j = 0; j < 3; j++) {
                    if (!model_iter.hasNext()) {
                        ASSERT_TRUE(!iter.valid());
                        break;
                    } else {
                        ASSERT_TRUE(iter.valid());
                        ASSERT_EQ(model_iter.next(), iter.key());
                        iter.next();
                    }
                }
            }

            // Backward iteration test
            {
                SkipListIterator<Integer, TestComparator> iter = new SkipListIterator<Integer, SkipList.TestComparator>(
                        list);
                iter.seekToLast();

                // Compare against model iterator
                Iterator<re_integer> model_iter = keys_reverse.iterator();
                for (; model_iter.hasNext(); ) {
                    ASSERT_TRUE(iter.valid());
                    ASSERT_EQ(model_iter.next().val, iter.key());
                    iter.prev();
                }
                ASSERT_TRUE(!iter.valid());
            }

        }


    }

    public static void main(String args[]) {
        SkiplistTest slt = new SkiplistTest();
        // slt.Empty();
        slt.insertAndLookup();
    }
}