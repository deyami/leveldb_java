package tests;

import com.leveldb.common.file.FileName;
import com.leveldb.common.file.FileType;
import static com.leveldb.common.file.FileName.*;

/**
 * @author tongfeng.dhy
 * @since 2015-04-26.
 */
public class FileNameTest {
    public void Parse() {
        FileType type = new FileType();
        long number = -1;

        // Successful parses
        class A {
            String fname;
            long number;
            int type;

            public A(String f, long n, int t) {
                fname = f;
                number = n;
                type = t;
            }
        }
        A cases[] = {
                new A("100.log", 100, FileType.kLogFile),
                new A("0.log", 0, FileType.kLogFile),
                new A("0.sst", 0, FileType.kTableFile),
                new A("CURRENT", 0, FileType.kCurrentFile),
                new A("LOCK", 0, FileType.kDBLockFile),
                new A("MANIFEST-2", 2, FileType.kDescriptorFile),
                new A("MANIFEST-7", 7, FileType.kDescriptorFile),
                new A("LOG", 0, FileType.kInfoLogFile),
                new A("LOG.old", 0, FileType.kInfoLogFile),
                new A("446744073709551615.log", 446744073709551615l,
                        FileType.kLogFile)};
        for (int i = 0; i < cases.length; i++) {
            String f = cases[i].fname;
            try {
                number = parseFileName(f, type);
            } catch (Exception e) {
                e.printStackTrace();
            }
            // ASSERT_TRUE(parseFileName(f, &number, &type)) << f;
            ASSERT_TRUE(cases[i].type == type.value, f);
            ASSERT_TRUE(cases[i].number == number, f);
        }

        // Errors
        String errors[] = {"", "foo", "foo-dx-100.log", ".log", "",
                "manifest", "CURREN", "CURRENTX", "MANIFES", "MANIFEST",
                "MANIFEST-", "XMANIFEST-3", "MANIFEST-3x", "LOC", "LOCKx",
                "LO", "LOGx", "18446744073709551616.log",
                "184467440737095516150.log", "100", "100.", "100.lop"};
        ASSERT_TRUE(true, "-------------------");
        for (int i = 0; i < errors.length; i++) {
            String f = errors[i];
            try {
                ASSERT_TRUE(parseFileName(f, type) != -1, f);
            } catch (Exception e) {
                ASSERT_TRUE(true, "error   " + f);
            }
        }
    }

    void Construction() {
        FileType type = new FileType();
        String fname;

        try {

            fname = currentFileName("foo");
            ASSERT_TRUE("foo/".compareTo(fname.substring(0, 4)) == 0, fname);
            ASSERT_TRUE(parseFileName(fname.substring(4), type) == 0,
                    fname.substring(4));

            ASSERT_TRUE(FileType.kCurrentFile == type.value, type.value
                    + "");

            fname = lockFileName("foo");
            ASSERT_TRUE("foo/".compareTo(fname.substring(0, 4)) == 0, fname);
            ASSERT_TRUE(parseFileName(fname.substring(4), type) == 0,
                    fname.substring(4));
            ASSERT_TRUE(FileType.kDBLockFile == type.value, type.value + "");

            fname = logFileName("foo", 192);
            ASSERT_TRUE("foo/".compareTo(fname.substring(0, 4)) == 0, fname);
            ASSERT_TRUE(parseFileName(fname.substring(4), type) == 192,
                    fname.substring(4));
            ASSERT_TRUE(FileType.kLogFile == type.value, type.value + "");

            fname = tableFileName("bar", 200);
            ASSERT_TRUE("bar/".compareTo(fname.substring(0, 4)) == 0, fname);
            ASSERT_TRUE(parseFileName(fname.substring(4), type) == 200,
                    fname.substring(4));
            ASSERT_TRUE(FileType.kTableFile == type.value, type.value + "");

            fname = descriptorFileName("bar", 100);
            ASSERT_TRUE("bar/".compareTo(fname.substring(0, 4)) == 0, fname);
            ASSERT_TRUE(parseFileName(fname.substring(4), type) == 100,
                    fname.substring(4));
            ASSERT_TRUE(FileType.kDescriptorFile == type.value, type.value
                    + "");

            fname = FileName.tempFileName("tmp", 999);
            ASSERT_TRUE("tmp/".compareTo(fname.substring(0, 4)) == 0, fname);
            ASSERT_TRUE(parseFileName(fname.substring(4), type) == 999,
                    fname.substring(4));
            ASSERT_TRUE(FileType.kTempFile == type.value, type.value + "");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void ASSERT_TRUE(boolean b, String f) {
        if (b) {
            System.out.println(f);
        }

    }

    public static void main(String args[]) {
        FileNameTest fnt = new FileNameTest();
        try {
            // fnt.Parse();
            fnt.Construction();
        } catch (Exception e) {
            // e.printStackTrace();
        }
    }
}
