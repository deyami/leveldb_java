package com.leveldb.util;

import com.leveldb.common.*;
import com.leveldb.common.file.*;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class DefaultEnv extends Env {
    /*
     *
     */
    private final ReentrantLock mutex = new ReentrantLock();
    private final Condition bgsignal_ = mutex.newCondition();

    private Thread bgthread_;// background thread
    private boolean started_bgthread_;

    private boolean stop_bgthread; // used to stop the ..
    /*
     * Functions in queue_ do not have args, values dealt with are set as member
     * variables
     */
    private Queue<Function> queue_;

    class BGThread extends Thread {
        public void run() {
            while (true) {
                //
                if (stop_bgthread) {
                    break;
                }
                mutex.lock();
                if (queue_.isEmpty()) {
                    try {
                        // wlu, 2012-6-2, so that the thread will be end b
                        // DB.close()
                        bgsignal_.await();
                    } catch (InterruptedException e) {
                        // e.printStackTrace();
                        System.out
                                .println("Thread is interrupted from outside.");
                    }
                }
                Function fun = queue_.poll();
                mutex.unlock();
                if (fun != null)
                    fun.exec();
            }
        }
    }

    public DefaultEnv() {
        started_bgthread_ = false; // not started yet
        stop_bgthread = false;
        queue_ = new ConcurrentLinkedQueue<Function>();
    }

    @Override
    public _SequentialFile newSequentialFile(String fname) {
        return new DefaultSequentialFile(fname);
    }

    @Override
    public _RandomAccessFile newRandomAccessFile(String fname) {
        try {
            return new DefaultRandomAccessFile(fname);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public _WritableFile newWritableFile(String fname) {
        return new DefaultWritableFile(fname, 4 << 10, 4 << 10);
    }

    @Override
    public boolean fileExists(String fname) {
        return (new File(fname)).exists();
    }

    @Override
    public List<String> getChildren(String dir) {
        File[] subFile = (new File(dir)).listFiles();
        if (subFile == null) {
            return new ArrayList<String>(0);
        }
        List<String> result = new ArrayList<String>(subFile.length);
        for (File sf : subFile) {
            result.add(sf.getName());
        }
        return result;
    }

    @Override
    public Status deleteFile(String fname) {
        try {

            FileUtils.forceDelete(new File(fname));
            // (new File(fname)).deleteOnExit();
        } catch (Exception e) {
            e.printStackTrace();
            // System.exit(1);
            return Status.ioerror(new Slice(e.toString()), null);
        }
        return Status.OK();
    }

    @Override
    public Status createDir(String dirname) {
        File d = new File(dirname);
        if (d.exists() && d.isDirectory()) {
            return Status.ioerror(new Slice("Same dir exists"), null);
        } else {
            d.mkdir();
        }
        return Status.OK();
    }

    /**
     * dirname MUST be a direction name
     */
    @Override
    public Status deleteDir(String dirname) {
        try {
            FileUtils.cleanDirectory(new File(dirname));
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return Status.ioerror(new Slice(e.getMessage()), null);
        }
        return Status.OK();

    }

    @Override
    public long getFileSize(String fname) {
        if (fileExists(fname)) {
            try {
                FileInputStream fis = new FileInputStream(new File(fname));
                return fis.available();
            } catch (IOException e) {
                e.printStackTrace();
                return -1;
            }
        } else {
            return -1;
        }
    }

    @Override
    public Status renameFile(String src, String target) {
        try {
            File file = new File(src);
            if (!file.renameTo(new File(target))) {
                FileUtils.copyFile(file, new File(target));
                FileUtils.forceDelete(file);
                // throw new IOException("Rename file from " + src + " to "
                // + target + " failed!");
            }
        } catch (Exception e) {
            return Status.ioerror(new Slice(e.toString()), null);
        }
        return Status.OK();
    }

    @Override
    public FileLock lockFile(String fname) {
        RandomAccessFile input = null;
        try {
            input = new RandomAccessFile(fname, "rw");
            FileChannel channel = input.getChannel();
            return channel.lock();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }

    }

    @Override
    public Status unlockFile(FileLock lock) {
        try {
            lock.release();
            lock.channel().close();
        } catch (IOException e) {
            e.printStackTrace();
            return Status.ioerror(new Slice("FileLock release fail"),
                    new Slice(e.toString()));
        }
        return Status.OK();
    }

    @Override
    public void schedule(Function fun) {
        mutex.lock();
        // start background thread if necessary
        if (!started_bgthread_) {
            started_bgthread_ = true;
            // wlu, 2012-7-8, bugfix: this flag should be reset for BGThread to
            // fire!!!
            stop_bgthread = false;
            bgthread_ = new BGThread();
            bgthread_.start();
        }
        // If the queue is currently empty, the background thread may currently
        // be waiting.
        if (queue_.isEmpty()) {
            bgsignal_.signalAll();
        }
        queue_.add(fun);
        mutex.unlock();
    }

    public void endSchedule() {
        stop_bgthread = true;
        // wlu, 2012-7-10, need to reset 'started_bgthread', otherwise, bg
        // thread will not be created again after database reopen on the fly
        started_bgthread_ = false;
        if (started_bgthread_ && bgthread_ != null) {
            bgthread_.interrupt();
        }

        // bgsignal_.signalAll();

    }

    @Override
    public void startThread(final Function fun) {
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                fun.exec();
            }
        };
        (new Thread(runnable)).start();
    }

    @Override
    public String getTestDirectory() {
        String path = "C:/tmp/leveldb-" + Thread.currentThread().getId();
        createDir(path);
        return path;
    }

    @Override
    public Logger newLogger(String fname) {
        return new DefaultLogger(new File(fname));
    }

    @Override
    public long nowMicros() {
        return (new Date()).getTime();
    }

    @Override
    public void sleepForMicroseconds(int micros) {
        try {
            Thread.sleep(micros);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    } // PosixEnv

}
