package com.leveldb.tests;

import com.leveldb.common.*;
import com.leveldb.common.comparator.BytewiseComparatorImpl;
import com.leveldb.common.comparator.InternalKeyComparator;
import com.leveldb.common.db.InternalKey;
import com.leveldb.common.db.MemTable;
import com.leveldb.common.db.ParsedInternalKey;
import com.leveldb.util.ValueType;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

public class WriteBatchTest extends TestCase {
    static void ASSERT_TRUE(boolean b) {
        if (!b) {
            System.out.println(b);
        }
        assertEquals(b, true);
    }

    static void ASSERT_TRUE(boolean b, String error) {
        if (!b) {
            System.out.println(error);
        }
        assertEquals(b, true);
    }

    static String PrintContents(WriteBatch b) {
        InternalKeyComparator cmp = new InternalKeyComparator(
                BytewiseComparatorImpl.getInstance());
        MemTable mem = new MemTable(cmp);
        mem.Ref();
        StringBuffer state = new StringBuffer();
        Status s = WriteBatchInternal.InsertInto(b, mem);
        int count = 0;
        Iterator iter = mem.NewIterator();
        for (iter.seekToFirst(); iter.valid(); iter.next()) {
            ParsedInternalKey ikey = InternalKey.ParseInternalKey_(iter.key());
            ASSERT_TRUE(ikey != null);
            switch (ikey.type.value) {
                case ValueType.kTypeValue:
                    state.append("put(");
                    state.append(ikey.user_key.toString());
                    state.append(", ");
                    state.append(iter.value().toString());
                    state.append(")");
                    count++;
                    break;
                case ValueType.kTypeDeletion:
                    state.append("delete(");
                    state.append(ikey.user_key.toString());
                    state.append(")");
                    count++;
                    break;
            }
            state.append("@");
            state.append(ikey.sequence.value);
        }
        iter = null;
        if (!s.ok()) {
            state.append("ParseError()");
        } else if (count != WriteBatchInternal.count(b)) {
            state.append("CountMismatch()");
        }
        mem.Unref();
        return state.toString();
    }

    public void testEmpty() {
        WriteBatch batch = new WriteBatch();
        ASSERT_TRUE("".compareTo(PrintContents(batch)) == 0);
        ASSERT_TRUE(0 == WriteBatchInternal.count(batch));
    }

    public void testMultiple() {
        WriteBatch batch = new WriteBatch();
        batch.put(new Slice("foo"), new Slice("bar"));
        batch.delete(new Slice("box"));
        batch.put(new Slice("baz"), new Slice("boo"));
        WriteBatchInternal.SetSequence(batch, 100);
        ASSERT_TRUE(100 == WriteBatchInternal.Sequence(batch).value);
        ASSERT_TRUE(3 == WriteBatchInternal.count(batch));
        ASSERT_TRUE(
                "put(baz, boo)@102Delete(box)@101Put(foo, bar)@100"
                        .compareTo(PrintContents(batch)) == 0,
                PrintContents(batch));
    }

    /**
     * This test throws an Exception: java.lang.ArrayIndexOutOfBoundsException.
     * Because the buffer is manually corrupted!
     */
    public void testCorruption() {
        WriteBatch batch = new WriteBatch();
        batch.put(new Slice("foo"), new Slice("bar"));
        batch.delete(new Slice("box"));
        WriteBatchInternal.SetSequence(batch, 200);
        Slice contents = WriteBatchInternal.Contents(batch);
        WriteBatchInternal.SetContents(batch, new Slice(contents.data(),
                contents.size() - 1));
        ASSERT_TRUE(
                "put(foo, bar)@200ParseError()".compareTo(PrintContents(batch)) == 0,
                PrintContents(batch));
    }

    public void testAppend() {
        WriteBatch b1 = new WriteBatch(), b2 = new WriteBatch();
        WriteBatchInternal.SetSequence(b1, 200);
        WriteBatchInternal.SetSequence(b2, 300);
        WriteBatchInternal.Append(b1, b2);
        ASSERT_TRUE("".compareTo(PrintContents(b1)) == 0);
        b2.put(new Slice("a"), new Slice("va"));
        WriteBatchInternal.Append(b1, b2);
        ASSERT_TRUE("put(a, va)@200".compareTo(PrintContents(b1)) == 0);
        b2.clear();
        b2.put(new Slice("b"), new Slice("vb"));
        WriteBatchInternal.Append(b1, b2);
        ASSERT_TRUE("put(a, va)@200Put(b, vb)@201".compareTo(PrintContents(b1)) == 0);
        b2.delete(new Slice("foo"));
        WriteBatchInternal.Append(b1, b2);
        ASSERT_TRUE("put(a, va)@200Put(b, vb)@202Put(b, vb)@201Delete(foo)@203"
                .compareTo(PrintContents(b1)) == 0);
    }

    public static void main(String args[]) {
        TestSuite t = new TestSuite(WriteBatchTest.class.getName());
        t.addTestSuite(WriteBatchTest.class);
        TestRunner.run(t);
    }
}
