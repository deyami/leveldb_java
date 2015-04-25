package com.leveldb.common;

import java.util.concurrent.atomic.AtomicReference;

// TODO: I am not sure about the difference between Barrier and NoBarrier
public class AtomicPointer<V> {
    private AtomicReference<V> rep_;

    public AtomicPointer(V p) {
        rep_ = new AtomicReference<V>(p);
    }

    public V noBarrierLoad() {
        return rep_.get();
    }

    public void noBarrierStore(V v) {
        rep_.set(v);
    }

    public V acquireLoad() {
//		AtomicReference<V> res = new AtomicReference<V>();
//		V v = rep_.get();
//		res.compareAndSet(v, v); // make sure res is completely read
//		return res.get();
        return noBarrierLoad();
    }

    public void releaseStore(V v) {
//		rep_.compareAndSet(v, v);	// make sure rep_ is completely set here
        noBarrierStore(v);
    }

}
