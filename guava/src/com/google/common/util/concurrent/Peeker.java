package com.google.common.util.concurrent;


import com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.*;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

/**
 * An object that can return the value of the {@link ClosingFuture}s that are passed to {@link
 * #whenAllComplete(Iterable)} or {@link #whenAllSucceed(Iterable)}.
 *
 * <p>Only for use by a {@link CombiningCallable} or {@link AsyncCombiningCallable} object.
 */
public final class Peeker {
    private final ImmutableList<ClosingFuture<?>> futures;
    private volatile boolean beingCalled;

    public Peeker(ImmutableList<ClosingFuture<?>> futures) {
        this.futures = checkNotNull(futures);
    }

    /**
     * Returns the value of {@code closingFuture}.
     *
     * @throws ExecutionException if {@code closingFuture} is a failed step
     * @throws CancellationException if the {@code closingFuture}'s future was cancelled
     * @throws IllegalArgumentException if {@code closingFuture} is not one of the futures passed to
     *     {@link #whenAllComplete(Iterable)} or {@link #whenAllComplete(Iterable)}
     * @throws IllegalStateException if called outside of a call to {@link
     *     CombiningCallable#call(DeferredCloser, Peeker)} or {@link
     *     AsyncCombiningCallable#call(DeferredCloser, Peeker)}
     */
    @ParametricNullness
    public  <D extends @Nullable Object> D getDone(ClosingFuture<D> closingFuture)
            throws ExecutionException {
        checkState(beingCalled);
        checkArgument(futures.contains(closingFuture));
        return Futures.getDone(closingFuture.future);
    }

    @ParametricNullness
    public <V extends @Nullable Object> V call(
            CombiningCallable<V> combiner, CloseableList closeables) throws Exception {
        beingCalled = true;
        CloseableList newCloseables = new CloseableList();
        try {
            return combiner.call(newCloseables.closer, this);
        } finally {
            closeables.add(newCloseables, directExecutor());
            beingCalled = false;
        }
    }

    public <V extends @Nullable Object> FluentFuture<V> callAsync(
            AsyncCombiningCallable<V> combiner, CloseableList closeables) throws Exception {
        beingCalled = true;
        CloseableList newCloseables = new CloseableList();
        try {
            ClosingFuture<V> closingFuture = combiner.call(newCloseables.closer, this);
            closingFuture.becomeSubsumedInto(closeables);
            return closingFuture.future;
        } finally {
            closeables.add(newCloseables, directExecutor());
            beingCalled = false;
        }
    }
}
