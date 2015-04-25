package com.leveldb.tests;

import com.leveldb.common.Slice;
import com.leveldb.common.Status;
import com.leveldb.common.db.InternalKey;
import com.leveldb.common.version.VersionEdit;
import com.leveldb.util.SequenceNumber;
import com.leveldb.util.ValueType;
import com.leveldb.util.util;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

public class VersionEditTest extends TestCase {
    // ////////////////////////////
    public static void TestEncodeDecode(VersionEdit edit) {
        byte[] encoded, encoded2;
        encoded = edit.encodeTo();
        VersionEdit parsed = new VersionEdit();
        Status s = parsed.decodeFrom(new Slice(encoded));
        System.out.println(s);
        // ASSERT_TRUE(s.ok()) << s.ToString();
        encoded2 = parsed.encodeTo();
        System.out.println(util.compareTo(encoded, encoded2));
        // ASSERT_EQ(encoded, encoded2);
    }

    public void testA() {
        long kBig = 1 << 50;

        VersionEdit edit = new VersionEdit();
        for (int i = 0; i < 4; i++) {
            TestEncodeDecode(edit);
            edit.addFile(3, kBig + 300 + i, kBig + 400 + i, new InternalKey(
                    new Slice("foo"), new SequenceNumber(kBig + 500 + i),
                    new ValueType(ValueType.kTypeValue)), new InternalKey(
                    new Slice("zoo"), new SequenceNumber(kBig + 600 + i),
                    new ValueType(ValueType.kTypeDeletion)));
            edit.deleteFile(4, kBig + 700 + i);

            edit.setCompactPointer(i, new InternalKey(new Slice("x"),
                    new SequenceNumber(kBig + 900 + i), new ValueType(
                    ValueType.kTypeValue)));
        }

        edit.setComparatorName(new Slice("foo"));
        edit.setLogNumber(kBig + 100);
        edit.setNextFile(kBig + 200);
        edit.setLastSequence(new SequenceNumber(kBig + 1000));
        TestEncodeDecode(edit);
    }

    public static void main(String args[]) {
        TestSuite t = new TestSuite("Version Edit Test");
        t.addTestSuite(VersionEditTest.class);
        TestRunner.run(t);
    }

}
