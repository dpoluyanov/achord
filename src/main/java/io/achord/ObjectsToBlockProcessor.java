package io.achord;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.EventLoop;
import io.netty.util.ReferenceCountUtil;

import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;

import static io.achord.DataBlock.EMPTY;

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
    // we can do better without synchronization and with avoiding all contention problems but something later
    // with another algorithm that could be applied as below:
    //      1) write into current block under cas
    //      2) if cas write is failed create new block and write into (but keep the same counter)
    //      3) after rows counter exceeded threshold (1024 * 1024) switch this couple of blocks into merge state
    //         note: neither new writes can be done to this blocks.
    //      4) when last thread writes into blocks in merge state (probably should be controlled by another counter)
    //         the thread pushes this blocks into subscriber onNext chain
    static int i = 0;
    private final AtomicLong requested = new AtomicLong(0);
    private final ByteBufAllocator alloc;
    private final EventLoop eventLoop;
    volatile DataBlock sample;
    private volatile Flow.Subscription subscription;
    private volatile int state = UNSUBSCRIBED;
    private volatile Flow.Subscriber<? super DataBlock> subscriber;
    private volatile ColumnWithTypeAndName[] columns;
    private volatile int rows;

    ObjectsToBlockProcessor(DataBlock sample, EventLoop eventLoop, ByteBufAllocator alloc) {
        this.sample = sample;
        this.eventLoop = eventLoop;
        this.alloc = alloc;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super DataBlock> subscriber) {
        this.subscriber = subscriber;
        recreateColumns();
        if (STATE.compareAndSet(this, SUBSCRIBED, WIP)) {
            subscriber.onSubscribe(new Flow.Subscription() {
                @Override
                public void request(long n) {
                    if (state == WIP) {
                        // suppose that in future some number of buffers should be on-the-fly for immediately sending after request
                        requested.addAndGet(n * BLOCK_SIZE);

                        // request data for n-next blocks
                        subscription.request(n * BLOCK_SIZE);
                    } else {
                        onError(new IllegalStateException("request(n) come to unexpected size"));
                    }
                }

                @Override
                public void cancel() {
                    // todo: later need rewrite without synchronizations
                    synchronized (ObjectsToBlockProcessor.this) {
                        if (subscription != null) {
                            try {
                                subscription.cancel();

                                STATE.set(ObjectsToBlockProcessor.this, CANCELLED);
                            } finally {
                                freeBuffers();
                            }
                        }
                    }
                }
            });
        } else {
            freeBuffers();
            throw new IllegalStateException("onSubscribe come to unexpected state");
        }
    }


    private synchronized void recreateColumns() {
        ColumnWithTypeAndName[] cs = new ColumnWithTypeAndName[sample.columns.length];
        for (int i = 0; i < sample.columns.length; i++) {
            // todo: data for buffers can be written independently (by some bunch of buffers)
            // and then collected into composite buffer (before forming resulting dataBlock)
            ByteBuf data = alloc.directBuffer();
            cs[i] = new ColumnWithTypeAndName(sample.columns[i].type, sample.columns[i].name, data);
        }
        columns = cs;
        rows = 0;
    }

    private synchronized void freeBuffers() {
        if (sample != null) {
            ReferenceCountUtil.release(sample);
            sample = null;
        }
        if (columns != null) {
            for (int i = 0; i < columns.length; i++) {
                // columns[i] can be null if error happens on creation stage
                if (columns[i] != null && columns[i].data.refCnt() > 0)
                    // release once
                    ReferenceCountUtil.release(columns[i].data);
            }

            columns = null;
        }
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
                            // block can does not reach any channel handler
                            if (block.refCnt() > 0)
                                ReferenceCountUtil.release(block);
                            throw e;
                        }
                        recreateColumns();
                    }
                } else {
                    throw new IllegalStateException("onNext produces unexpected count of elements");
                }
            } else {
                // possible should be treated as ignore
                throw new IllegalStateException("onNext passed to illegal state");
            }
        } catch (Throwable e) {
            onError(e);
        }
    }

    @Override
    public void onError(Throwable throwable) {
        try {
            STATE.set(this, ERROR);
            subscription.cancel();
            subscriber.onError(throwable);
        } finally {
            freeBuffers();
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
            subscriber.onNext((DataBlock) EMPTY.retain());
        }
        subscriber.onComplete();
    }
}
