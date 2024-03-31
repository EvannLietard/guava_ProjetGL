package com.google.common.util.concurrent;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

/**
 * A builder of a {@link ClosingFuture} step that is derived from more than one input step.
 *
 * <p>See {@link #whenAllComplete(Iterable)} and {@link #whenAllSucceed(Iterable)} for how to
 * instantiate this class.
 *
 * <p>Example:
 *
 * <pre>{@code
 * final ClosingFuture<BufferedReader> file1ReaderFuture = ...;
 * final ClosingFuture<BufferedReader> file2ReaderFuture = ...;
 * ListenableFuture<Integer> numberOfDifferentLines =
 *       ClosingFuture.whenAllSucceed(file1ReaderFuture, file2ReaderFuture)
 *           .call(
 *               (closer, peeker) -> {
 *                 BufferedReader file1Reader = peeker.getDone(file1ReaderFuture);
 *                 BufferedReader file2Reader = peeker.getDone(file2ReaderFuture);
 *                 return countDifferentLines(file1Reader, file2Reader);
 *               },
 *               executor)
 *           .closing(executor);
 * }</pre>
 */
// TODO(cpovirk): Use simple name instead of fully qualified after we stop building with JDK 8.
@com.google.errorprone.annotations.DoNotMock(
        "Use ClosingFuture.whenAllSucceed() or .whenAllComplete() instead.")
public class Combiner {

    private final CloseableList closeables = new CloseableList();

    private final boolean allMustSucceed;
    protected final ImmutableList<ClosingFuture<?>> inputs;

    public Combiner(boolean allMustSucceed, Iterable<? extends ClosingFuture<?>> inputs) {
        this.allMustSucceed = allMustSucceed;
        this.inputs = ImmutableList.copyOf(inputs);
        for (ClosingFuture<?> input : inputs) {
            input.becomeSubsumedInto(closeables);
        }
    }

    /**
     * Returns a new {@code ClosingFuture} pipeline step derived from the inputs by applying a
     * combining function to their values. The function can use a {@link DeferredCloser} to capture
     * objects to be closed when the pipeline is done.
     *
     * <p>If this combiner was returned by a {@link #whenAllSucceed} method and any of the inputs
     * fail, so will the returned step.
     *
     * <p>If the combiningCallable throws a {@code CancellationException}, the pipeline will be
     * cancelled.
     *
     * <p>If the combiningCallable throws an {@code ExecutionException}, the cause of the thrown
     * {@code ExecutionException} will be extracted and used as the failure of the derived step.
     */
    public <V extends @Nullable Object> ClosingFuture<V> call(
            final CombiningCallable<V> combiningCallable, Executor executor) {
        Callable<V> callable =
                new Callable<V>() {
                    @Override
                    @ParametricNullness
                    public V call() throws Exception {
                        return new Peeker(inputs).call(combiningCallable, closeables);
                    }

                    @Override
                    public String toString() {
                        return combiningCallable.toString();
                    }
                };
        ClosingFuture<V> derived = new ClosingFuture<>(futureCombiner().call(callable, executor));
        derived.closeables.add(closeables, directExecutor());
        return derived;
    }

    /**
     * Returns a new {@code ClosingFuture} pipeline step derived from the inputs by applying a
     * {@code ClosingFuture}-returning function to their values. The function can use a {@link
     * DeferredCloser} to capture objects to be closed when the pipeline is done (other than those
     * captured by the returned {@link ClosingFuture}).
     *
     * <p>If this combiner was returned by a {@link #whenAllSucceed} method and any of the inputs
     * fail, so will the returned step.
     *
     * <p>If the combiningCallable throws a {@code CancellationException}, the pipeline will be
     * cancelled.
     *
     * <p>If the combiningCallable throws an {@code ExecutionException}, the cause of the thrown
     * {@code ExecutionException} will be extracted and used as the failure of the derived step.
     *
     * <p>If the combiningCallable throws any other exception, it will be used as the failure of the
     * derived step.
     *
     * <p>If an exception is thrown after the combiningCallable creates a {@code ClosingFuture},
     * then none of the closeable objects in that {@code ClosingFuture} will be closed.
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
     *       ClosingFuture} call {@link #from(ListenableFuture)}.
     * </ul>
     *
     * <p>The same warnings about doing heavyweight operations within {@link
     * ClosingFuture#transformAsync(AsyncClosingFunction, Executor)} apply here.
     */
    public <V extends @Nullable Object> ClosingFuture<V> callAsync(
            final AsyncCombiningCallable<V> combiningCallable, Executor executor) {
        AsyncCallable<V> asyncCallable =
                new AsyncCallable<V>() {
                    @Override
                    public ListenableFuture<V> call() throws Exception {
                        return new Peeker(inputs).callAsync(combiningCallable, closeables);
                    }

                    @Override
                    public String toString() {
                        return combiningCallable.toString();
                    }
                };
        ClosingFuture<V> derived =
                new ClosingFuture<>(futureCombiner().callAsync(asyncCallable, executor));
        derived.closeables.add(closeables, directExecutor());
        return derived;
    }

    private Futures.FutureCombiner<@Nullable Object> futureCombiner() {
        return allMustSucceed
                ? Futures.whenAllSucceed(inputFutures())
                : Futures.whenAllComplete(inputFutures());
    }


    private ImmutableList<FluentFuture<?>> inputFutures() {
        return FluentIterable.from(inputs)
                .<FluentFuture<?>>transform(future -> future.future)
                .toList();
    }
}
