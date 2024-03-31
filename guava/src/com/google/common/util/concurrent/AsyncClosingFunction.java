package com.google.common.util.concurrent;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.concurrent.Executor;

/**
 * A function from an input to a {@link ClosingFuture} of a result.
 *
 * @param <T> the type of the input to the function
 * @param <U> the type of the result of the function
 */
@FunctionalInterface
public interface AsyncClosingFunction<T extends @Nullable Object, U extends @Nullable Object> {
    /**
     * Applies this function to an input, or throws an exception if unable to do so.
     *
     * <p>Any objects that are passed to {@link DeferredCloser#eventuallyClose(Object, Executor)
     * closer.eventuallyClose()} will be closed when the {@link ClosingFuture} pipeline is done (but
     * not before this method completes), even if this method throws or the pipeline is cancelled.
     */
    ClosingFuture<U> apply(DeferredCloser closer, @ParametricNullness T input) throws Exception;
}
