package com.google.common.util.concurrent;

import org.checkerframework.checker.nullness.qual.Nullable;

import javax.annotation.CheckForNull;
import java.io.Closeable;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.Platform.restoreInterruptIfIsInterruptedException;
import static java.util.logging.Level.WARNING;

// TODO(dpb): Should we use a pair of ArrayLists instead of an IdentityHashMap?
public  final class CloseableList extends IdentityHashMap<AutoCloseable, Executor>
        implements Closeable {
    public final DeferredCloser closer = new DeferredCloser(this);
    private static final LazyLogger logger = new LazyLogger(ClosingFuture.class);
    private volatile boolean closed;
    @CheckForNull
    private volatile CountDownLatch whenClosed;

    <V extends @Nullable Object, U extends @Nullable Object>
    ListenableFuture<U> applyClosingFunction(
            ClosingFunction<? super V, U> transformation, @ParametricNullness V input)
            throws Exception {
        // TODO(dpb): Consider ways to defer closing without creating a separate CloseableList.
        CloseableList newCloseables = new CloseableList();
        try {
            return immediateFuture(transformation.apply(newCloseables.closer, input));
        } finally {
            add(newCloseables, directExecutor());
        }
    }

    <V extends @Nullable Object, U extends @Nullable Object>
    FluentFuture<U> applyAsyncClosingFunction(
            AsyncClosingFunction<V, U> transformation, @ParametricNullness V input)
            throws Exception {
        // TODO(dpb): Consider ways to defer closing without creating a separate CloseableList.
        CloseableList newCloseables = new CloseableList();
        try {
            ClosingFuture<U> closingFuture = transformation.apply(newCloseables.closer, input);
            closingFuture.becomeSubsumedInto(newCloseables);
            return closingFuture.future;
        } finally {
            add(newCloseables, directExecutor());
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
        }
        for (Map.Entry<AutoCloseable, Executor> entry : entrySet()) {
            closeQuietly(entry.getKey(), entry.getValue());
        }
        clear();
        if (whenClosed != null) {
            whenClosed.countDown();
        }
    }

    public void add(@CheckForNull AutoCloseable closeable, Executor executor) {
        checkNotNull(executor);
        if (closeable == null) {
            return;
        }
        synchronized (this) {
            if (!closed) {
                put(closeable, executor);
                return;
            }
        }
        closeQuietly(closeable, executor);
    }

    /**
     * Returns a latch that reaches zero when this objects' deferred closeables have been closed.
     */
    CountDownLatch whenClosedCountDown() {
        if (closed) {
            return new CountDownLatch(0);
        }
        synchronized (this) {
            if (closed) {
                return new CountDownLatch(0);
            }
            checkState(whenClosed == null);
            return whenClosed = new CountDownLatch(1);
        }
    }
    private static void closeQuietly(@CheckForNull final AutoCloseable closeable, Executor executor) {
        if (closeable == null) {
            return;
        }
        try {
            executor.execute(
                    () -> {
                        try {
                            closeable.close();
                        } catch (Exception e) {
                            /*
                             * In guava-jre, any kind of Exception may be thrown because `closeable` has type
                             * `AutoCloseable`.
                             *
                             * In guava-android, the only kinds of Exception that may be thrown are
                             * RuntimeException and IOException because `closeable` has type `Closeable`—except
                             * that we have to account for sneaky checked exception.
                             */
                            restoreInterruptIfIsInterruptedException(e);
                            logger.get().log(WARNING, "thrown by close()", e);
                        }
                    });
        } catch (RejectedExecutionException e) {
            if (logger.get().isLoggable(WARNING)) {
                logger
                        .get()
                        .log(
                                WARNING,
                                String.format("while submitting close to %s; will close inline", executor),
                                e);
            }
            closeQuietly(closeable, directExecutor());
        }
    }
    @Override
    public boolean equals(Object obj){
        if (this == obj) {
            return true;
        }
        if (obj == null || this.getClass() != obj.getClass()) {
            return false;
        }
        CloseableList other = (CloseableList) obj;
        if (this.closed != other.closed||this.size() != other.size()) {
            return false;
        }
        for (Map.Entry<AutoCloseable, Executor> entry : entrySet()) {
            AutoCloseable key = entry.getKey();
            Executor value = entry.getValue();
            if (!other.containsKey(key) || !value.equals(other.get(key))) {
                return false;
            }
        }
        return true;

    }
}
