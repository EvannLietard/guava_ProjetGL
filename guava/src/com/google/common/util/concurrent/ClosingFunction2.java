package com.google.common.util.concurrent;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.concurrent.Executor;

/**
 * A function that returns a value when applied to the values of the two futures passed to
 * {@link #whenAllSucceed(ClosingFuture, ClosingFuture)}.
 *
 * @param <V1> the type returned by the first future
 * @param <V2> the type returned by the second future
 * @param <U> the type returned by the function
 */
@FunctionalInterface
public interface ClosingFunction2<
        V1 extends @Nullable Object, V2 extends @Nullable Object, U extends @Nullable Object> {

    /**
     * Applies this function to two inputs, or throws an exception if unable to do so.
     *
     * <p>Any objects that are passed to {@link DeferredCloser#eventuallyClose(Object, Executor)
     * closer.eventuallyClose()} will be closed when the {@link ClosingFuture} pipeline is done
     * (but not before this method completes), even if this method throws or the pipeline is
     * cancelled.
     */
    @ParametricNullness
    U apply(DeferredCloser closer, @ParametricNullness V1 value1, @ParametricNullness V2 value2)
            throws Exception;
}
