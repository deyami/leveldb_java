package com.leveldb.common;

import com.leveldb.common.file._RandomAccessFile;
import com.leveldb.common.file._SequentialFile;
import com.leveldb.common.file._WritableFile;

import java.nio.channels.FileLock;
import java.util.List;

public class EnvWrapper extends Env {
    // Initialize an EnvWrapper that delegates all calls to *target
    public EnvWrapper(Env target) {
        target_ = target;
    }

    // Return the target to which this Env forwards all calls
    public Env target() {
        return target_;
    }

    // The following text is boilerplate that forwards all methods to target()
    public _SequentialFile newSequentialFile(String f) {
        return target_.newSequentialFile(f);
    }

    public _RandomAccessFile newRandomAccessFile(String f) {
        return target_.newRandomAccessFile(f);
    }

    public _WritableFile newWritableFile(String f) {
        return target_.newWritableFile(f);
    }

    public boolean fileExists(String f) {
        return target_.fileExists(f);
    }

    public List<String> getChildren(String dir) {
        return target_.getChildren(dir);
    }

    public Status deleteFile(String f) {
        return target_.deleteFile(f);
    }

    public Status createDir(String d) {
        return target_.createDir(d);
    }

    public Status deleteDir(String d) {
        return target_.deleteDir(d);
    }

    public long getFileSize(String f) {
        return target_.getFileSize(f);
    }

    public Status renameFile(String s, String t) {
        return target_.renameFile(s, t);
    }

    public FileLock lockFile(String f) {
        return target_.lockFile(f);
    }

    public Status unlockFile(FileLock l) {
        return target_.unlockFile(l);
    }

    public void schedule(Function fun) {
        target_.schedule(fun);
    }

    public void startThread(Function fun) {
        target_.startThread(fun);
    }

    public String getTestDirectory() {
        return target_.getTestDirectory();
    }


    public long nowMicros() {
        return target_.nowMicros();
    }

    public void sleepForMicroseconds(int micros) {
        target_.sleepForMicroseconds(micros);
    }


    @Override
    public Logger newLogger(String fname) {
        return target_.newLogger(fname);
    }

    private Env target_;
}