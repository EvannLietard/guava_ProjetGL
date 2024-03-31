package com.google.common.util.concurrent;

import com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.concurrent.Executor;

/**
 * A generic {@link Combiner} that lets you use a lambda or method reference to combine four
 * {@link ClosingFuture}s. Use {@link ClosingFuture#whenAllSucceed(ClosingFuture, ClosingFuture, ClosingFuture,
 * ClosingFuture)} to start this combination.
 *
 * @param <V1> the type returned by the first future
 * @param <V2> the type returned by the second future
 * @param <V3> the type returned by the third future
 * @param <V4> the type returned by the fourth future
 */
public  final class Combiner4<
        V1 extends @Nullable Object,
        V2 extends @Nullable Object,
        V3 extends @Nullable Object,
        V4 extends @Nullable Object>
        extends Combiner {




    private final ClosingFuture<V1> future1;
    private final ClosingFuture<V2> future2;
    private final ClosingFuture<V3> future3;
    private final ClosingFuture<V4> future4;

    public Combiner4(
            ClosingFuture<V1> future1,
            ClosingFuture<V2> future2,
            ClosingFuture<V3> future3,
            ClosingFuture<V4> future4) {
        super(true, ImmutableList.of(future1, future2, future3, future4));
        this.future1 = future1;
        this.future2 = future2;
        this.future3 = future3;
        this.future4 = future4;
    }

    /**
     * Returns a new {@code ClosingFuture} pipeline step derived from the inputs by applying a
     * combining function to their values. The function can use a {@link DeferredCloser} to capture
     * objects to be closed when the pipeline is done.
     *
     * <p>If this combiner was returned by {@link ClosingFuture#whenAllSucceed(ClosingFuture, ClosingFuture,
     * ClosingFuture, ClosingFuture)} and any of the inputs fail, so will the returned step.
     *
     * <p>If the function throws a {@code CancellationException}, the pipeline will be cancelled.
     *
     * <p>If the function throws an {@code ExecutionException}, the cause of the thrown {@code
     * ExecutionException} will be extracted and used as the failure of the derived step.
     */
    public <U extends @Nullable Object> ClosingFuture<U> call(
            final ClosingFunction4<V1, V2, V3, V4, U> function, Executor executor) {
        return call(
                new CombiningCallable<U>() {
                    @Override
                    @ParametricNullness
                    public U call(DeferredCloser closer, Peeker peeker) throws Exception {
                        return function.apply(
                                closer,
                                peeker.getDone(future1),
                                peeker.getDone(future2),
                                peeker.getDone(future3),
                                peeker.getDone(future4));
                    }

                    @Override
                    public String toString() {
                        return function.toString();
                    }
                },
                executor);
    }

    /**
     * Returns a new {@code ClosingFuture} pipeline step derived from the inputs by applying a
     * {@code ClosingFuture}-returning function to their values. The function can use a {@link
     * DeferredCloser} to capture objects to be closed when the pipeline is done (other than those
     * captured by the returned {@link ClosingFuture}).
     *
     * <p>If this combiner was returned by {@link ClosingFuture#whenAllSucceed(ClosingFuture, ClosingFuture,
     * ClosingFuture, ClosingFuture)} and any of the inputs fail, so will the returned step.
     *
     * <p>If the function throws a {@code CancellationException}, the pipeline will be cancelled.
     *
     * <p>If the function throws an {@code ExecutionException}, the cause of the thrown {@code
     * ExecutionException} will be extracted and used as the failure of the derived step.
     *
     * <p>If the function throws any other exception, it will be used as the failure of the derived
     * step.
     *
     * <p>If an exception is thrown after the function creates a {@code ClosingFuture}, then none of
     * the closeable objects in that {@code ClosingFuture} will be closed.
     *
     * <p>Usage guidelines for this method:
     *
     * <ul>
     *   <li>Use this method only when calling an API that returns a {@link ListenableFuture} or a
     *       {@code ClosingFuture}. If possible, prefer calling {@link #call(CombiningCallable,
     *       Executor)} instead, with a function that returns the next value directly.
     *   <li>Call {@link DeferredCloser#eventuallyClose(Object, Executor) closer.eventuallyClose()}
     *       for every closeable object this step creates in order to capture it for later closing.
     *   <li>Return a {@code ClosingFuture}. To turn a {@link ListenableFuture} into a {@code
     *       ClosingFuture} call {@link ClosingFuture#from(ListenableFuture)}.
     * </ul>
     *
     * <p>The same warnings about doing heavyweight operations within {@link
     * ClosingFuture#transformAsync(AsyncClosingFunction, Executor)} apply here.
     */
    public <U extends @Nullable Object> ClosingFuture<U> callAsync(
            final AsyncClosingFunction4<V1, V2, V3, V4, U> function, Executor executor) {
        return callAsync(
                new AsyncCombiningCallable<U>() {
                    @Override
                    public ClosingFuture<U> call(DeferredCloser closer, Peeker peeker) throws Exception {
                        return function.apply(
                                closer,
                                peeker.getDone(future1),
                                peeker.getDone(future2),
                                peeker.getDone(future3),
                                peeker.getDone(future4));
                    }

                    @Override
                    public String toString() {
                        return function.toString();
                    }
                },
                executor);
    }
}

