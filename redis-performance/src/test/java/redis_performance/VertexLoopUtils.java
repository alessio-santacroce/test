/*
 * Copyright (C) 2016 AlertMe.com Ltd
 */

package redis_performance;

import io.vertx.core.Vertx;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class VertexLoopUtils {

    private VertexLoopUtils() {
    }

    @FunctionalInterface
    public interface OnTaskCompleted {
        void completed();
    }

    @FunctionalInterface
    public interface OnLoopCompleted {
        void completed();
    }

    @FunctionalInterface
    public interface AsyncTask {
        void work(final OnTaskCompleted onTaskCompleted);
    }

    public static class VertexLoop {
        private final OnTaskCompleted onTaskCompleted;
        private final int concurrency;

        public VertexLoop(final Vertx vertx, final Supplier<Optional<AsyncTask>> taskSupplier,
                          final int concurrency, final OnLoopCompleted onLoopCompleted) {
            this.concurrency = concurrency;
            final AtomicInteger countDown = new AtomicInteger(concurrency);
            this.onTaskCompleted = new OnTaskCompleted() {
                @Override
                public void completed() {
                    final Optional<AsyncTask> taskOpt = taskSupplier.get();
                    if (taskOpt.isPresent()) {
                        vertx.runOnContext(v -> taskOpt.get().work(this));
                    } else {
                        if (countDown.decrementAndGet() <= 0) {
                            onLoopCompleted.completed();
                        }
                    }
                }
            };
        }

        public void start() {
            for (int i = 0; i < concurrency; i++) {
                onTaskCompleted.completed();
            }
        }
    }

//    private VertexLoopUtils(final Vertx vertx, final Supplier<Optional<AsyncTask>> taskSupplier,
//                            final int concurrency, final OnLoopCompleted onLoopCompleted) {
//        this.concurrency = concurrency;
//        final AtomicInteger countDown = new AtomicInteger(concurrency);
//        this.onTaskCompleted = new OnTaskCompleted() {
//            @Override
//            public void completed() {
//                final Optional<AsyncTask> taskOpt = taskSupplier.get();
//                if (taskOpt.isPresent()) {
//                    vertx.runOnContext(v -> taskOpt.get().work(this));
//                } else {
//                    if (countDown.decrementAndGet() <= 0) {
//                        onLoopCompleted.completed();
//                    }
//                }
//            }
//        };
//    }


    public static void loop(final Vertx vertx, final Supplier<Optional<AsyncTask>> taskSupplier,
                            final int concurrency, final OnLoopCompleted onLoopCompleted) {
        new VertexLoop(vertx, taskSupplier, concurrency, onLoopCompleted).start();
    }

    public static void loop(final Vertx vertx, final int repetitions, final int concurrency, final AsyncTask task,
                            final OnLoopCompleted onLoopCompleted) {
        loop(vertx, buildTaskSupplier(repetitions, task), concurrency, onLoopCompleted);
    }

    public static Supplier<Optional<AsyncTask>> buildTaskSupplier(final int repetitions, final AsyncTask task) {
        final AtomicInteger counter = new AtomicInteger(-1);
        final Optional<AsyncTask> taskOpt = Optional.of(task);
        return () -> {
            final int i = counter.incrementAndGet();
            return (i < repetitions) ? taskOpt : Optional.empty();
        };
    }

    public static Supplier<Optional<AsyncTask>> buildTaskSupplier(final AsyncTask... tasks) {
        final AtomicInteger counter = new AtomicInteger(-1);
        return () -> {
            final int i = counter.incrementAndGet();
            return (i < tasks.length) ? Optional.of(tasks[i]) : Optional.empty();
        };
    }
}
