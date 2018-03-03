package io.achord;

import java.util.concurrent.Flow;

/**
 * @author Camelion
 * @since 11/02/2018
 */
final class VoidPublisher implements Flow.Publisher<Void> {
    private Flow.Subscriber<? super Void> subscriber;
    private Subscription subscription;

    @Override
    public void subscribe(Flow.Subscriber<? super Void> subscriber) {
        this.subscriber = subscriber;
        subscriber.onSubscribe((subscription = new Subscription()));
    }

    void complete() {
        Flow.Subscriber<? super Void> s;

        if ((s = subscriber) != null) {
            s.onComplete();
        }
    }

    void error(Throwable error) {
        Flow.Subscriber<? super Void> s;

        if ((s = subscriber) != null) {
            s.onError(error);
        }
    }

    static class Subscription implements Flow.Subscription {

        @Override
        public void request(long n) {
            /* no-op */
        }

        @Override
        public void cancel() {

        }
    }
}
