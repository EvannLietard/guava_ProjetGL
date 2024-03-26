package com.google.common.util.concurrent;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.j2objc.annotations.RetainedWith;
import org.checkerframework.checker.nullness.qual.Nullable;


import java.util.concurrent.Executor;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * An object that can capture objects to be closed later, when a {@link ClosingFuture} pipeline is
 * done.
 */
public final class DeferredCloser {
    @RetainedWith
    private final ClosingFuture.CloseableList list;

    DeferredCloser(ClosingFuture.CloseableList list) {
        this.list = list;
    }

    /**
     * Captures an object to be closed when a {@link ClosingFuture} pipeline is done.
     *
     * <p>For users of the {@code -jre} flavor of Guava, the object can be any {@code
     * AutoCloseable}. For users of the {@code -android} flavor, the object must be a {@code
     * Closeable}. (For more about the flavors, see <a
     * href="https://github.com/google/guava#adding-guava-to-your-build">Adding Guava to your
     * build</a>.)
     *
     * <p>Be careful when targeting an older SDK than you are building against (most commonly when
     * building for Android): Ensure that any object you pass implements the interface not just in
     * your current SDK version but also at the oldest version you support. For example, <a
     * href="https://developer.android.com/sdk/api_diff/16/">API Level 16</a> is the first version
     * in which {@code Cursor} is {@code Closeable}. To support older versions, pass a wrapper
     * {@code Closeable} with a method reference like {@code cursor::close}.
     *
     * <p>Note that this method is still binary-compatible between flavors because the erasure of
     * its parameter type is {@code Object}, not {@code AutoCloseable} or {@code Closeable}.
     *
     * @param closeable the object to be closed (see notes above)
     * @param closingExecutor the object will be closed on this executor
     * @return the first argument
     */
    @CanIgnoreReturnValue
    @ParametricNullness
    public <C extends @Nullable Object & @Nullable AutoCloseable> C eventuallyClose(
            @ParametricNullness C closeable, Executor closingExecutor) {
        checkNotNull(closingExecutor);
        if (closeable != null) {
            list.add(closeable, closingExecutor);
        }
        return closeable;
    }
}
