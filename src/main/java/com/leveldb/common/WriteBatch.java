package com.leveldb.common;

import com.leveldb.common.db.MemTable;
import com.leveldb.util.SequenceNumber;
import com.leveldb.util.ValueType;
import com.leveldb.util.coding;
import com.leveldb.util.util;

//WriteBatch::rep_ :=
//sequence: fixed64
//count: fixed32
//data: record[count]
//record :=
//kTypeValue varstring varstring         |
//kTypeDeletion varstring
//varstring :=
//len: varint32
//data: uint8[len]


/**
 * write key-value Slice to memtable in a batch mode
 * 2012-4-26, test cases of WriteBatch is added
 */
public class WriteBatch {
    // WriteBatch header has an 8-byte sequence number followed by a 4-byte
    // count.
    public static int kHeader = 12;
    public ByteCollection rep_;

    public WriteBatch() {
        rep_ = new ByteCollection(null, -1);
        clear();
    }

    /**
     * Store the mapping "key->value" in the database. append:
     * TypeValue|key|value
     *
     * @param key
     * @param value
     */
    public void put(Slice key, Slice value) { //
        WriteBatchInternal.SetCount(this, WriteBatchInternal.count(this) + 1);
        rep_.bytes = util.add(rep_.bytes, util.toBytes(ValueType.kTypeValue),
                coding.putLengthPrefixedSlice(key),
                coding.putLengthPrefixedSlice(value));
    }

    // If the database contains a mapping for "key", erase it. Else do nothing.
    public void delete(Slice key) {//
        WriteBatchInternal.SetCount(this, WriteBatchInternal.count(this) + 1);
        rep_.bytes = util.add(rep_.bytes,
                util.toBytes(ValueType.kTypeDeletion),
                coding.putLengthPrefixedSlice(key));
    }

    // clear all updates buffered in this batch.
    public void clear() {
        rep_.bytes = new byte[kHeader];
    }

    // Support for iterating over the contents of a batch.
    public abstract class Handler {
        abstract void put(Slice key, Slice value);

        abstract void delete(Slice key);
    }

    /**
     * iterate over all records (#=count) and put them by the Handler
     *
     * @param handler {@see #Handler}
     * @return
     */
    public Status iterate(Handler handler) {
        Slice input = new Slice(rep_.bytes);
        rep_.curr_pos = 0;
        if (input.size() < kHeader) {
            return Status.corruption(new Slice(
                    "malformed WriteBatch (too small)"), null);
        }

        // input.remove_prefix(kHeader);
        rep_.curr_pos += kHeader;
        Slice key, value;
        int found = 0;
        while (!rep_.STOP()) {
            found++;
            byte tag = rep_.get();// input.get(0);
            // input.remove_prefix(1);
            rep_.curr_pos += 1;
            switch (tag) {
                case ValueType.kTypeValue:
                    try {
                        key = coding.getLengthPrefixedSlice(rep_);
                        value = coding.getLengthPrefixedSlice(rep_);
                        handler.put(key, value);
                    } catch (Exception e) {
                        e.printStackTrace();
                        return Status.corruption(new Slice("bad WriteBatch put"),
                                null);
                    }
                    break;
                case ValueType.kTypeDeletion:
                    try {
                        key = coding.getLengthPrefixedSlice(rep_);

                        handler.delete(key);
                    } catch (Exception e) {
                        e.printStackTrace();
                        return Status.corruption(
                                new Slice("bad WriteBatch delete"), null);
                    }
                    break;
                default:
                    return Status.corruption(new Slice("unknown WriteBatch tag"),
                            null);
            }
        }
        if (found != WriteBatchInternal.count(this)) {
            return Status.corruption(new Slice("WriteBatch has wrong count"),
                    null);
        } else {
            return Status.OK();
        }
    }

    // private String rep_; // See comment in write_batch.cc for the format of
    // rep_

    // Intentionally copyable
    public class MemTableInserter extends Handler {
        public MemTableInserter() {
            super();
        }

        SequenceNumber sequence_;
        MemTable mem_;

        void put(Slice key, Slice value) {
            mem_.Add(sequence_, ValueType.kTypeValue, key, value);
            sequence_.value++;
        }

        void delete(Slice key) {
            mem_.Add(sequence_, ValueType.kTypeDeletion, key, new Slice());
            sequence_.value++;
        }

    }

    public MemTableInserter inserter = new MemTableInserter();


}
