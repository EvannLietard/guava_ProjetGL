package com.google.common.util.concurrent;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.concurrent.Executor;

/**
 * An operation that computes a {@link ClosingFuture} of a result.
 *
 * @param <V> the type of the result
 * @since 30.1
 */
@FunctionalInterface
public interface AsyncClosingCallable<V extends @Nullable Object> {
    /**
     * Computes a result, or throws an exception if unable to do so.
     *
     * <p>Any objects that are passed to {@link DeferredCloser#eventuallyClose(Object, Executor)
     * closer.eventuallyClose()} will be closed when the {@link ClosingFuture} pipeline is done (but
     * not before this method completes), even if this method throws or the pipeline is cancelled.
     */
    ClosingFuture<V> call(DeferredCloser closer) throws Exception;
}

/**
 * A function from an input to a result.
 *
 * @param <T> the type of the input to the function
 * @param <U> the type of the result of the function
 */
