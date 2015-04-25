package com.leveldb.util;

import com.leveldb.common.ByteCollection;
import com.leveldb.common.Slice;

import java.util.Random;

public class coding {

    // TODO: performance is not good, may be caused by arraycopy...

    public static byte[] encodeVarint32(int v) {
        byte[] des = null;
        int B = 128;
        if (v < (1 << 7)) {
            des = new byte[1];
            des[0] = (byte) v;
        } else if (v < (1 << 14)) {
            des = new byte[2];
            des[0] = (byte) (v | B); // low
            des[1] = (byte) (v >> 7); // high
        } else if (v < (1 << 21)) {
            des = new byte[3];
            des[0] = (byte) (v | B);
            des[1] = (byte) ((v >> 7) | B);
            des[2] = (byte) (v >> 14);
        } else if (v < (1 << 28)) {
            des = new byte[4];
            des[0] = (byte) (v | B);
            des[1] = (byte) ((v >> 7) | B);
            des[2] = (byte) ((v >> 14) | B);
            des[3] = (byte) (v >> 21);
        } else {
            des = new byte[5];
            des[0] = (byte) (v | B);
            des[1] = (byte) ((v >> 7) | B);
            des[2] = (byte) ((v >> 14) | B);
            des[3] = (byte) ((v >> 21) | B);
            des[4] = (byte) (v >> 28);
        }

        return des;
    }

    // actually just return the byte[] of Varint
    public static byte[] putVarint32(int v) {
        return encodeVarint32(v);
    }

    // bytes of length: bytes of data
    public static byte[] putLengthPrefixedSlice(Slice value) {
        byte[] sz_ = encodeVarint32(value.size());
        byte[] dt_ = value.data();

        return util.add(sz_, dt_);

    }

    public static byte[] putLengthPrefixedBytes(byte[] value) {
        byte[] sz_ = encodeVarint32(value.length);
        byte[] dt_ = value;

        return util.add(sz_, dt_);
    }

    // public static Slice getLengthPrefixedSlice(Slice islice) {
    // byte[] vlen_data = islice.data();
    // Pair<Integer, Integer> v_l = getVarint32(vlen_data, 0);
    // int len = v_l.getFirst().intValue();
    // int ofs = v_l.getSecond().intValue();
    // byte[] data = new byte[len];
    // System.arraycopy(vlen_data, ofs, data, 0, len);
    // return new Slice(data);
    //
    // }

    /**
     * get a length-prefixes-Slice begin from the byte @ src.curr_pos
     * side-effect: move curr_pos to the end of the Slice
     */
    public static Slice getLengthPrefixedSlice(ByteCollection src) {
        byte[] vlen_data = src.bytes;
        int len = getVarint32(src);
        if (!src.OK()) {
            return null;
        }
        byte[] data = new byte[len];
        System.arraycopy(vlen_data, src.curr_pos, data, 0, len);
        src.curr_pos += len;
        return new Slice(data);

    }

    public static Slice getLengthPrefixedSlice(byte[] vlen_data) {
        return getLengthPrefixedSlice(new ByteCollection(vlen_data, 0));
    }

    // [value, length]
    public static int getVarint32(ByteCollection src) {
        int value = 0;
        int length = 1;
        int shift = 0;
        int offset = src.curr_pos;
        byte[] src_ = src.bytes;
        try {
            for (int i = offset; ; i++) {
                value += (src_[i] & 0x7f) * (1 << (7 * shift));
                shift++;
                if ((src_[i] & 128) == 0) {
                    break;
                } else {
                    length++;
                }
            }
        } catch (ArrayIndexOutOfBoundsException e) {
            src.ok = false;
            return 0;
        }
        src.curr_pos += length;
        return value;
    }

    // just return the value
    public static int getVarint32(byte[] src_, int offset) {
        int value = 0;
        int shift = 0;

        try {
            for (int i = offset; ; i++) {
                value += (src_[i] & 0x7f) * (1 << (7 * shift));
                shift++;
                if ((src_[i] & 128) == 0) {
                    break;
                } else {
                }
            }
        } catch (ArrayIndexOutOfBoundsException e) {

            return 0;
        }
        return value;
    }

    public static void test_varint32() {
        byte[] s;
        int[] va = {23, (1 << 7) + 23, (1 << 14) + (1 << 7) + 23,
                (1 << 21) + (1 << 14) + (1 << 7) + 23};
        for (int i : va) {
            int v = i;
            s = encodeVarint32(v);
            int j = getVarint32(new ByteCollection(s, 0));
            if (i != j) {
                System.out.println("bad");
            }
        }

        Random rn = new Random();
        for (int i = 0; i < 10000; i++) {
            int v = rn.nextInt();
            v = v & ((1 << 31) - 1);
            s = encodeVarint32(v);
            int j = getVarint32(new ByteCollection(s, 0));
            if (v != j) {
                System.out.println("bad: " + v);
            }
        }

    }

    public static byte[] encodeVarint64(long v) {
        int B = 128;
        byte[] bb = new byte[0];
        while (v >= B) {
            byte[] ba = new byte[1];
            ba[0] = (byte) ((v & (B - 1)) | B);
            bb = util.add(bb, ba);
            v >>= 7;
        }
        byte[] ba = new byte[1];
        ba[0] = (byte) (v);
        bb = util.add(bb, ba);

        return bb;
    }

    public static byte[] putVarint64(long v) {
        return encodeVarint64(v);
    }

    public static long getVarint64(ByteCollection src) {
        long value = 0;
        int length = 1;
        int shift = 0;
        int offset = src.curr_pos;
        byte[] src_ = src.bytes;
        try {
            for (int i = offset; ; i++) {
                value += (src_[i] & 0x7f) * (1 << (7 * shift));
                shift++;
                if ((src_[i] & 128) == 0) {
                    break;
                } else {
                    length++;
                }
            }
        } catch (ArrayIndexOutOfBoundsException e) {
            src.ok = false;
            return 0;//whatever...
        }
        src.curr_pos += length;
        return value;
    }

    // get the length of is varint coding bytes
    public static int varintLength(long v) {
        int len = 1;
        while (v >= 128) {
            v >>= 7;
            len++;
        }
        return len;
    }

    public static void test_varint64() {
        byte[] s;
        Random rn = new Random();
        for (int i = 0; i < 10000; i++) {
            long v = rn.nextLong();
            v = (v & ((1 << 63) - 1));
            s = encodeVarint64(v);
            long j = getVarint64(new ByteCollection(s, 0));
            if (v != j) {
                System.out.println("bad: " + v);
            }
        }
    }

    public static void test_slice() {
        String str = new String();
        for (int i = 0; i < 5000; i++) {
            str += "this is a test of slice";
        }
        Slice s = new Slice(str.getBytes());
        Slice r = getLengthPrefixedSlice(new ByteCollection(
                putLengthPrefixedSlice(s), 0));
        if (s.compareTo(r) != 0) {
            System.out.println("bad: " + s.toString().length() + "\t"
                    + r.toString().length());
        }
    }

    public static void test_combined() {
        int iv = 1234;
        long iL = 10023456l;
        String is = "this is a string";
        byte[] en = util.addN(putVarint32(iv), putVarint64(iL),
                putLengthPrefixedSlice(new Slice(is)));
        int pos = 0;

        ByteCollection bc = new ByteCollection(en, 0);
        int ov = getVarint32(bc);

        long oL = getVarint64(bc);

        Slice os = getLengthPrefixedSlice(bc);

        System.out.println(ov + " " + oL + " " + os);
    }

    public static void main(String args[]) {
        // test_varint64();
        // test_slice();
        for (int i = 0; i < 10; i++)
            test_combined();
        int i = 200000;
        byte b[] = putVarint32(i);
        int i_ = getVarint32(new byte[]{-64, -102, 12}, 0);
        System.out.println(i_);
//		byte b_ = -48;
//		int bi = b_&0xff;
//		System.out.println(bi);
    }

}
