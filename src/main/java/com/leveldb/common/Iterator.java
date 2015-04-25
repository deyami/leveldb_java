package com.leveldb.common;

class EmptyIterator extends Iterator {
    EmptyIterator(Status s) {
        status_ = s;
    }

    public boolean valid() {
        return false;
    }

    public void seek(Slice target) {
    }

    public void seekToFirst() {
    }

    public void seekToLast() {
    }

    public void next() {
        assert (false);
    }

    public void prev() {
        assert (false);
    }

    public Slice key() {
        assert (false);
        return new Slice();
    }

    public Slice value() {
        assert (false);
        return new Slice();
    }

    public Status status() {
        return status_;
    }

    Status status_;
}

public abstract class Iterator {

    // An iterator is either positioned at a key/value pair, or
    // not valid. This method returns true iff the iterator is valid.
    public abstract boolean valid();

    // Position at the first key in the source. The iterator is valid()
    // after this call iff the source is not empty.
    public abstract void seekToFirst();

    // Position at the last key in the source. The iterator is
    // valid() after this call iff the source is not empty.
    public abstract void seekToLast();

    // Position at the first key in the source that at or past target
    // The iterator is valid() after this call iff the source contains
    // an entry that comes at or past target.
    public abstract void seek(Slice target);

    // Moves to the next entry in the source. After this call, valid() is
    // true iff the iterator was not positioned at the last entry in the source.
    // REQUIRES: valid()
    public abstract void next();

    // Moves to the previous entry in the source. After this call, valid() is
    // true iff the iterator was not positioned at the first entry in source.
    // REQUIRES: valid()
    public abstract void prev();

    // Return the key for the current entry. The underlying storage for
    // the returned slice is valid only until the next modification of
    // the iterator.
    // REQUIRES: valid()
    public abstract Slice key();

    // Return the value for the current entry. The underlying storage for
    // the returned slice is valid only until the next modification of
    // the iterator.
    // REQUIRES: !AtEnd() !AtStart()
    public abstract Slice value();

    // If an error has occurred, return it. Else return an ok status.
    public abstract Status status();

    // Clients are allowed to register function/arg1/arg2 triples that
    // will be invoked when this iterator is destroyed.
    //
    // Note that unlike all of the preceding methods, this method is
    // not abstract and therefore clients should not override it.

    public void registerCleanup(Function func, Object arg1, Object arg2) {
        assert (func != null);
        cleanup c;
        if (cleanup_.function == null) {
            c = cleanup_;
        } else {
            c = new cleanup();
            c.next = cleanup_.next;
            cleanup_.next = c;
        }
        c.function = func;
        c.arg1 = arg1;
        c.arg2 = arg2;
        ;
    }

    class cleanup {
        Function function;
        Object arg1;
        Object arg2;
        cleanup next;
    }

    protected cleanup cleanup_ = new cleanup();

    // No copying allowed

    // Return an empty iterator (yields nothing).
    public static Iterator newEmptyIterator() {
        return new EmptyIterator(Status.OK());
    }

    // Return an empty iterator with the specified status.
    public static Iterator newErrorIterator(Status status) {
        return new EmptyIterator(status);
    }
}
