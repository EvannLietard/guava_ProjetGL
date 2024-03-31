package com.google.common.util.concurrent;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.util.concurrent.Futures.getDone;

/**
 * An object that holds the final result of an asynchronous {@link ClosingFuture} operation and
 * allows the user to close all the closeable objects that were captured during it for later
 * closing.
 *
 * <p>The asynchronous operation will have completed before this object is created.
 *
 * @param <V> the type of the value of a successful operation
 * @see ClosingFuture#finishToValueAndCloser(ClosingFuture.ValueAndCloserConsumer, Executor)
 */
public final class ValueAndCloser<V extends @Nullable Object> {

    private final ClosingFuture<? extends V> closingFuture;

    ValueAndCloser(ClosingFuture<? extends V> closingFuture) {
        this.closingFuture = checkNotNull(closingFuture);
    }

    /**
     * Returns the final value of the associated {@link ClosingFuture}, or throws an exception as
     * {@link Future#get()} would.
     *
     * <p>Because the asynchronous operation has already completed, this method is synchronous and
     * returns immediately.
     *
     * @throws CancellationException if the computation was cancelled
     * @throws ExecutionException if the computation threw an exception
     */
    @ParametricNullness
    public V get() throws ExecutionException {
        return getDone(closingFuture.future);
    }

    /**
     * Starts closing all closeable objects captured during the {@link ClosingFuture}'s asynchronous
     * operation on the {@link Executor}s specified by calls to {@link
     * DeferredCloser#eventuallyClose(Object, Executor)}.
     *
     * <p>If any such calls specified {@link MoreExecutors#directExecutor()}, those objects will be
     * closed synchronously.
     *
     * <p>Idempotent: objects will be closed at most once.
     */
    public void closeAsync() {
        closingFuture.close();
    }
}

