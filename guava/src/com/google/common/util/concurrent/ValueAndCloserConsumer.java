package com.google.common.util.concurrent;


import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.concurrent.Executor;

/**
 * Represents an operation that accepts a {@link ValueAndCloser} for the last step in a {@link
 * ClosingFuture} pipeline.
 *
 * @param <V> the type of the final value of a successful pipeline
 * @see ClosingFuture#finishToValueAndCloser(ValueAndCloserConsumer, Executor)
 */
@FunctionalInterface
public interface ValueAndCloserConsumer<V extends @Nullable Object> {

    /** Accepts a {@link ValueAndCloser} for the last step in a {@link ClosingFuture} pipeline. */
    void accept(ValueAndCloser<V> valueAndCloser);
}
