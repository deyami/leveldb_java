package com.leveldb.common;

import com.leveldb.common.file._RandomAccessFile;
import com.leveldb.common.file._SequentialFile;
import com.leveldb.common.file._WritableFile;
import com.leveldb.util.DefaultEnv;

import java.nio.channels.FileLock;
import java.util.List;

//import com.leveldb.common.file.FileLock;

public abstract class Env {
    public Env() {
    }

    // Return a default environment suitable for the current operating
    // system. Sophisticated users may wish to provide their own Env
    // implementation instead of relying on this default environment.
    //
    // The result of Default() belongs to leveldb and must never be deleted.
    // TODO
    public static Env Default() {
        // TODO
        // add configuration file
        return new DefaultEnv();
    }

    ;

    // Create a brand new sequentially-readable file with the specified name.
    // On success, stores a pointer to the new file in *result and returns OK.
    // On failure stores NULL in *result and returns non-OK. If the file does
    // not exist, returns a non-OK status.
    //
    // The returned file will only be accessed by one thread at a time.
    public abstract _SequentialFile newSequentialFile(String fname); // wlu

    // Create a brand new random access read-only file with the
    // specified name. On success, stores a pointer to the new file in
    // *result and returns OK. On failure stores NULL in *result and
    // returns non-OK. If the file does not exist, returns a non-OK
    // status.
    //
    // The returned file may be concurrently accessed by multiple threads.
    public abstract _RandomAccessFile newRandomAccessFile(String fname);

    // Create an object that writes to a new file with the specified
    // name. Deletes any existing file with the same name and creates a
    // new file. On success, stores a pointer to the new file in
    // *result and returns OK. On failure stores NULL in *result and
    // returns non-OK.
    //
    // The returned file will only be accessed by one thread at a time.
    public abstract _WritableFile newWritableFile(String fname);

    // Returns true iff the named file exists.
    public abstract boolean fileExists(String fname);

    // Store in *result the names of the children of the specified directory.
    // The names are relative to "dir".
    // Original contents of *results are dropped.
    public abstract List<String> getChildren(String dir);

    // delete the named file.
    public abstract Status deleteFile(String fname);

    // Create the specified directory.
    public abstract Status createDir(String dirname);

    // delete the specified directory.
    public abstract Status deleteDir(String dirname);

    // Store the size of fname in *file_size.
    public abstract long getFileSize(String fname);

    // Rename file src to target.
    public abstract Status renameFile(String src, String target);

    // Lock the specified file. Used to prevent concurrent access to
    // the same db by multiple processes. On failure, stores NULL in
    // *lock and returns non-OK.
    //
    // On success, stores a pointer to the object that represents the
    // acquired lock in *lock and returns OK. The caller should call
    // unlockFile(*lock) to release the lock. If the process exits,
    // the lock will be automatically released.
    //
    // If somebody else already holds the lock, finishes immediately
    // with a failure. I.e., this call does not wait for existing locks
    // to go away.
    //
    // May create the named file if it does not already exist.
    public abstract FileLock lockFile(String fname);

    // release the lock acquired by a previous successful call to lockFile.
    // REQUIRES: lock was returned by a successful lockFile() call
    // REQUIRES: lock has not already been unlocked.
    public abstract Status unlockFile(FileLock lock);

    // Arrange to run "(*function)(arg)" once in a background thread.
    //
    // "function" may run in an unspecified thread. Multiple functions
    // added to the same Env may run concurrently in different threads.
    // I.e., the caller may not assume that background work items are
    // serialized.
    // void schedule(
    // void (*function)(void* arg),
    // void* arg) ;
    public abstract void schedule(Function fun);

    public void endSchedule() {
    }

    ;

    // Start a new thread, invoking "function(arg)" within the new thread.
    // When "function(arg)" returns, the thread will be destroyed.
    public abstract void startThread(Function fun);

    // *path is set to a temporary directory that can be used for testing. It
    // may
    // or many not have just been created. The directory may or may not differ
    // between runs of the same process, but subsequent calls will return the
    // same directory.
    public abstract String getTestDirectory();

    // Create and return a log file for storing informational messages.
    public abstract Logger newLogger(String fname);

    // Write an entry to the log file with the specified format.
    // public abstract void Logv(_WritableFile log, String format, Object...
    // ap);

    // Returns the number of micro-seconds since some fixed point in time. Only
    // useful for computing deltas of time.
    public abstract long nowMicros();

    // Sleep/delay the thread for the perscribed number of micro-seconds.
    public abstract void sleepForMicroseconds(int micros);

    // Log the specified data to *info_log if info_log is non-NULL.
    // TODO
    public static void Log(Logger logger, String... msg) {
        logger.Logv(msg[0], msg);
    }

    // A utility routine: write "data" to the named file.
    // TODO
    public static Status writeStringToFile(Env env, Slice data, String fname) {
        return null;
        /*
		 * env.cc WritableFile* file; Status s = env->newWritableFile(fname,
		 * &file); if (!s.ok()) { return s; } s = file->Append(data); if
		 * (s.ok()) { s = file->Close(); } delete file; // Will auto-close if we
		 * did not close above if (!s.ok()) { env->deleteFile(fname); } return
		 * s;
		 */
    }

    public static Status writeStringToFileSync(Env env, Slice data, String fname) {
        return doWriteStringToFile(env, data, fname, true);
    }

    public static Status doWriteStringToFile(Env env, Slice data, String fname,
                                             boolean should_sync) {
        _WritableFile file = env.newWritableFile(fname);

        Status s = file.Append(data);
        if (s.ok() && should_sync) {
            s = file.Sync();
        }
        if (s.ok()) {
            s = file.Close();
        }
        file = null; // Will auto-close if we did not close above
        if (!s.ok()) {
            env.deleteFile(fname);
        }
        return s;
    }

    // A utility routine: read contents of named file into *data
    public static String readFileToString(Env env, String fname) {
        ByteVector data = new ByteVector(128);
        _SequentialFile file = env.newSequentialFile(fname);

        int kBufferSize = 8192;
        while (true) {
            Slice fragment = new Slice();
            file.Read(kBufferSize, fragment);

            data.append(fragment.data());
            if (fragment.empty()) {
                break;
            }
        }

        file.Close();
        return data.toString();
    }
}