package io.achord;

import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ReferenceCountUtil;

import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Camelion
 * @since 19/02/2018
 */
final class ObjectsToBlockProcessor implements Flow.Processor<Object[], DataBlock> {
    private static final int BLOCK_SIZE = 1024 * 1024;
    private static final int UNSUBSCRIBED = -1;
    private static final int SUBSCRIBED = 0;
    private static final int WIP = 1;
    private static final int COMPLETED = 2;
    private static final int ERROR = 3;
    private static final int CANCELLED = 4;
    private static final AtomicIntegerFieldUpdater<ObjectsToBlockProcessor> STATE = AtomicIntegerFieldUpdater.newUpdater(ObjectsToBlockProcessor.class, "state");
    private final AtomicLong requested = new AtomicLong(0);
    private final ByteBufAllocator alloc;
    volatile DataBlock sample;
    private volatile Flow.Subscription subscription;
    private volatile int state = UNSUBSCRIBED;
    private volatile Flow.Subscriber<? super DataBlock> subscriber;

    private volatile ColumnWithTypeAndName[] columns;
    private volatile int rows;

    ObjectsToBlockProcessor(DataBlock sample, ByteBufAllocator alloc) {
        this.sample = sample;
        this.alloc = alloc;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super DataBlock> subscriber) {
        this.subscriber = subscriber;
        recreateColumns();
        subscriber.onSubscribe(new Flow.Subscription() {
            @Override
            public void request(long n) {
                try {
                    if (state == WIP) {
                        // suppose that in future some number of buffers should be on-the-fly for immediately sending after request
                        requested.addAndGet(n);

                        // request data for n-next blocks
                        subscription.request(n * BLOCK_SIZE);
                    } else {
                        throw new IllegalStateException("request(n) come to unexpected size");
                    }
                } catch (Throwable e) {
                    freeBuffers();
                    throw e;
                }
            }

            @Override
            public void cancel() {
                if (subscription != null) {
                    try {
                        subscription.cancel();

                        STATE.set(ObjectsToBlockProcessor.this, CANCELLED);
                    } finally {
                        freeBuffers();
                    }
                }
            }
        });
    }

    private void recreateColumns() {
        columns = new ColumnWithTypeAndName[sample.columns.length];
        for (int i = 0; i < sample.columns.length; i++) {
            // todo: data for buffers can be written independently (by some bunch of buffers)
            // and then collected into composite buffer (before forming resulting dataBlock)
            columns[i] = new ColumnWithTypeAndName(sample.columns[i].type, sample.columns[i].name, alloc.directBuffer());
        }
        rows = 0;
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        try {
            if (STATE.compareAndSet(this, UNSUBSCRIBED, SUBSCRIBED)) {
                this.subscription = subscription;
            } else {
                throw new IllegalStateException("State expected to be unsubscribed");
            }
        } catch (Throwable e) {
            freeBuffers();
            throw e;
        }
    }

    // we can do better without synchronization and with avoiding all contention problems but something later
    @Override
    public synchronized void onNext(Object[] item) {
        try {
            if (state == WIP) {
                long left;
                if ((left = requested.decrementAndGet()) >= 0) {
                    for (int i = 0; i < columns.length; i++) {
                        ColumnWithTypeAndName c = columns[i];
                        c.type.write(c.data, item[i]);
                    }
                    rows++;

                    if (rows >= BLOCK_SIZE) {
                        DataBlock block = new DataBlock(sample.info, columns, rows);

                        try {
                            subscriber.onNext(block);
                        } catch (Throwable e) {
                            // block does not reach any channel handler
                            ReferenceCountUtil.release(block);
                            throw e;
                        }
                        recreateColumns();
                        subscription.request(BLOCK_SIZE);
                    }
                } else {
                    throw new IllegalStateException("onNext produces unexpected count of elements");
                }
            } else {
                throw new IllegalStateException("onNext passed to illegal state");
            }
        } catch (Throwable e) {
            freeBuffers();
            throw e;
        }
    }

    @Override
    public void onError(Throwable throwable) {
        try {
            STATE.set(this, ERROR);
            subscriber.onError(throwable);
        } finally {
            freeBuffers();
        }
    }

    private synchronized void freeBuffers() {
        if (sample != null) {
            ReferenceCountUtil.release(sample);
            sample = null;
        }
        if (columns != null) {
            for (int i = 0; i < columns.length; i++) {
                ReferenceCountUtil.release(columns[i].data);
            }

            columns = null;
        }
    }

    @Override
    public void onComplete() {
        try {
            STATE.set(this, COMPLETED);
            flushLast();
        } finally {
            freeBuffers();
        }
    }

    private void flushLast() {
        if (rows > 0) {
            DataBlock block = new DataBlock(sample.info, columns, rows);

            subscriber.onNext(block);
        }
        subscriber.onComplete();
    }
}
