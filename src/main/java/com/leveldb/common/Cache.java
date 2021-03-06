package com.leveldb.common;

import com.leveldb.util.LRUCache;

public abstract class Cache {

    // Opaque handle to an entry stored in the cache.
    public abstract class Handle {

    }

    // insert a mapping from key->value into the cache and assign it
    // the specified charge against the total cache capacity.
    //
    // Returns a handle that corresponds to the mapping. The caller
    // must call this->release(handle) when the returned mapping is no
    // longer needed.
    //
    // When the inserted entry is no longer needed, the key and
    // value will be passed to "deleter".
    public abstract Handle insert(Slice key, Object value, int charge,
                                  Function deleter);

    // If the cache has no mapping for "key", returns NULL.
    //
    // Else return a handle that corresponds to the mapping. The caller
    // must call this->release(handle) when the returned mapping is no
    // longer needed.
    public abstract Handle lookup(Slice key);

    // release a mapping returned by a previous lookup().
    // REQUIRES: handle must not have been released yet.
    // REQUIRES: handle must have been returned by a method on *this.
    public abstract void release(Handle handle);

    // Return the value encapsulated in a handle returned by a
    // successful lookup().
    // REQUIRES: handle must not have been released yet.
    // REQUIRES: handle must have been returned by a method on *this.
    public abstract Object value(Handle handle);

    // If the cache contains entry for key, erase it. Note that the
    // underlying entry will be kept around until all existing handles
    // to it have been released.
    public abstract void erase(Slice key);

    // Return a new numeric id. May be used by multiple clients who are
    // sharing the same cache to partition the key space. Typically the
    // client will allocate a new id at startup and prepend the id to
    // its cache keys.
    public abstract long newId();

    public abstract void LRU_Remove(Handle e);

    public abstract void LRU_Append(Handle e);

    public abstract void Unref(Handle e);

    // wlu: 2012-6-2, destroy those left
    public abstract void destroy();

    class Rep {
    };

    Rep rep_;

    // No copying allowed
    public static Cache newLRUCache(int capacity) {
        return new LRUCache(capacity);
    }
}
