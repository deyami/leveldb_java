package com.leveldb.common.db;

import com.leveldb.common.*;
import com.leveldb.common.file.FileType;
import com.leveldb.common.file._WritableFile;
import com.leveldb.common.file.FileName;
import com.leveldb.common.log.Writer;
import com.leveldb.common.options.Options;
import com.leveldb.common.options.ReadOptions;
import com.leveldb.common.options.WriteOptions;
import com.leveldb.common.version.VersionEdit;

import java.nio.channels.FileLock;
import java.util.ArrayList;
import java.util.List;

//A DB is a persistent ordered map from keys to values.
//A DB is safe for concurrent access from multiple threads without
//any external synchronization.

public abstract class DB {
    // open the database with the specified "name".
    // Stores a pointer to a heap-allocated database in dbptr and returns
    // OK on success.
    // Stores NULL in dbptr and returns a non-OK status on error.
    // Caller should delete dbptr when it is no longer needed.

    // Set the database entry for "key" to "value". Returns OK on success,
    // and a non-OK status on error.
    // Note: consider setting options.sync = true.
    public Status put(WriteOptions opt, Slice key, Slice value) {
        WriteBatch batch = new WriteBatch();
        batch.put(key, value);
        return write(opt, batch); // call concrete write
    }

    // Remove the database entry (if any) for "key". Returns OK on
    // success, and a non-OK status on error. It is not an error if "key"
    // did not exist in the database.
    // Note: consider setting options.sync = true.
    public Status delete(WriteOptions opt, Slice key) {
        WriteBatch batch = new WriteBatch();
        batch.delete(key);
        return write(opt, batch);
    }

    // Apply the specified updates to the database.
    // Returns OK on success, non-OK on failure.
    // Note: consider setting options.sync = true.
    public abstract Status write(WriteOptions options, WriteBatch updates);

    // If the database contains an entry for "key" store the
    // corresponding value in value and return OK.
    //
    // If there is no entry for "key" leave value unchanged and return
    // a status for which Status::isNotFound() returns true.
    //
    // May return some other Status on an error.
    public abstract Slice get(ReadOptions options, Slice key, Status s);

    // Return a heap-allocated iterator over the contents of the database.
    // The result of newIterator() is initially invalid (caller must
    // call one of the seek methods on the iterator before using it).
    //
    // Caller should delete the iterator when it is no longer needed.
    // The returned iterator should be deleted before this db is deleted.
    public abstract Iterator newIterator(ReadOptions options);

    // Return a handle to the current DB state. Iterators created with
    // this handle will all observe a stable snapshot of the current DB
    // state. The caller must call releaseSnapshot(result) when the
    // snapshot is no longer needed.
    public abstract Snapshot getSnapshot();

    // release a previously acquired snapshot. The caller must not
    // use "snapshot" after this call.
    public abstract void releaseSnapshot(Snapshot snapshot);

    // DB implementations can export properties about their state
    // via this method. If "property" is a valid property understood by this
    // DB implementation, fills "value" with its current value and returns
    // true. Otherwise returns false.
    //
    //
    // valid property names include:
    //
    // "leveldb.num-files-at-level<N>" - return the number of files at level
    // <N>,
    // where <N> is an ASCII representation of a level number (e.g. "0").
    // "leveldb.stats" - returns a multi-line string that describes statistics
    // about the internal operation of the DB.
    public abstract boolean getProperty(Slice property, StringBuffer value);

    // For each i in [0,n-1], store in "sizes[i]", the approximate
    // file system space used by keys in "[range[i].start .. range[i].limit)".
    //
    // Note that the returned sizes measure file system space usage, so
    // if the user data compresses by a factor of ten, the returned
    // sizes will be one-tenth the size of the corresponding user data size.
    //
    // The results may not include the sizes of recently written data.
    public abstract long[] getApproximateSizes(Range range[], int n);

    public long getApproximateSizes(Range range) {
        return getApproximateSizes(new Range[]{range}, 1)[0];
    }

    public abstract void compactRange(Slice begin, Slice end);

    // Possible extensions:
    // (1) Add a method to compact a range of keys

    public static DB open(Options options, String dbname) {
        DB dbptr = null;
        DBImpl impl = new DBImpl(options, dbname);
        impl.getmutex().lock();
        VersionEdit edit = new VersionEdit();
        Status s = impl.Recover(edit); // Handles create_if_missing,
        // error_if_exists
        if (s.ok()) {
            long new_log_number = impl.versions_.NewFileNumber();
            _WritableFile lfile = options.env.newWritableFile(FileName
                    .logFileName(dbname, new_log_number));
            {
                edit.setLogNumber(new_log_number);
                impl.logfile_ = lfile;
                impl.logfile_number_ = new_log_number;
                impl.log_ = new Writer(lfile);
                s = impl.versions_.LogAndApply(edit, impl.mutex_);
            }
            if (s.ok()) {
                impl.DeleteObsoleteFiles();
                impl.MaybeScheduleCompaction();
            }
        }
        impl.mutex_.unlock();
        if (s.ok()) {
            dbptr = impl;
        } else {
            // wlu, 2012-7-10, bugFix: something goes wrong, release resources
            impl.close();
            impl = null;
        }
        return dbptr;
    }

    public static DB open(Options options, String dbname, Status s_) {
        DB dbptr = null;
        DBImpl impl = new DBImpl(options, dbname);
        impl.getmutex().lock();
        VersionEdit edit = new VersionEdit();
        Status s = impl.Recover(edit); // Handles create_if_missing,
        // error_if_exists
        if (s.ok()) {
            long new_log_number = impl.versions_.NewFileNumber();
            _WritableFile lfile = options.env.newWritableFile(FileName
                    .logFileName(dbname, new_log_number));
            {
                edit.setLogNumber(new_log_number);
                impl.logfile_ = lfile;
                impl.logfile_number_ = new_log_number;
                impl.log_ = new Writer(lfile);
                s = impl.versions_.LogAndApply(edit, impl.mutex_);
            }
            if (s.ok()) {
                impl.DeleteObsoleteFiles();
                impl.MaybeScheduleCompaction();
            }
        }
        impl.mutex_.unlock();
        if (s.ok()) {
            dbptr = impl;
        } else {
            // wlu, 2012-7-10, bugFix: something goes wrong, release resources
            impl.close();
            impl = null;

        }

        s_.Status_(s);
        return dbptr;
    }

    public abstract void close();

    public static Status destroyDB(String dbname, Options options) {
        Env env = options.env;
        List<String> filenames = new ArrayList<String>();
        // Ignore error in case directory does not exist
        try {
            filenames = env.getChildren(dbname);
        } catch (Exception e) {
            filenames = new ArrayList<String>();
        }
        if (filenames.isEmpty()) {
            return Status.OK();
        }

        FileLock lock = null;
        String lockname = FileName.lockFileName(dbname);
        lock = env.lockFile(lockname);

        Status s = Status.OK();

        if (lock != null) {
            long number;
            FileType type = new FileType();
            for (int i = 0; i < filenames.size(); i++) {
                try {
                    number = FileName.parseFileName(filenames.get(i), type);
                } catch (Exception e) {
                    // delete the file whatever it is
                    number = 0;
                }
                if (number >= 0 && type.value != FileType.kDBLockFile) { // Lock
                    // file
                    // will
                    // be
                    // deleted
                    // at
                    // end
                    s = env.deleteFile(dbname + "/" + filenames.get(i));
                }
            }
            env.unlockFile(lock); // Ignore error since state is already gone
            env.deleteFile(dbname + "/LOCK");
            env.deleteDir(dbname); // Ignore error in case dir contains other
            // files
        }
        return s;
    }

}
