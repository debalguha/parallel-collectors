package com.pivovarit.collectors;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

final class CancellableCompletableFuture<T> extends CompletableFuture<T> {
    private final CompletableFuture<T> source;
    private final Future<?> backingTask;

    private CancellableCompletableFuture(CompletableFuture<T> source, Future<?> backingTask) {
        this.source = source;
        this.backingTask = backingTask;
    }

    public static <U> CancellableCompletableFuture<U> supplyAsync(Supplier<U> supplier, ExecutorService executor) {
        CompletableFuture<U> result = new CompletableFuture<>();
        Future<?> caught = executor.submit(() -> {
            try {
                result.complete(supplier.get());
            } catch (Throwable e) {
                result.completeExceptionally(e);
            }
        });

        return from(result, caught);
    }

    static <T> CancellableCompletableFuture<T> from(CompletableFuture<T> source, Future<?> backingTask) {
        return new CancellableCompletableFuture<>(source, backingTask);
    }

    <U> CompletableFuture<U> from(CompletableFuture<U> source) {
        return new CancellableCompletableFuture<>(source, backingTask);
    }

    @Override

    public boolean complete(T value) {
        try {
            return source.complete(value);
        } finally {
            backingTask.cancel(true);
        }
    }

    @Override
    public boolean isDone() {
        return source.isDone();
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
        return source.get();
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return source.get(timeout, unit);
    }

    @Override
    public T join() {
        System.out.println("joining...");
        return source.join();
    }


    @Override
    public T getNow(T valueIfAbsent) {
        return source.getNow(valueIfAbsent);
    }

    @Override
    public boolean completeExceptionally(Throwable ex) {
        return source.completeExceptionally(ex);
    }

    @Override
    public <U> CompletableFuture<U> thenApply(Function<? super T, ? extends U> fn) {
        return from(source.thenApply(fn));
    }

    @Override
    public <U> CompletableFuture<U> thenApplyAsync(Function<? super T, ? extends U> fn) {
        return from(source.thenApplyAsync(fn));
    }

    @Override
    public <U> CompletableFuture<U> thenApplyAsync(Function<? super T, ? extends U> fn, Executor executor) {
        return from(source.thenApplyAsync(fn, executor));
    }

    @Override
    public CompletableFuture<Void> thenAccept(Consumer<? super T> action) {
        return from(source.thenAccept(action));
    }

    @Override
    public CompletableFuture<Void> thenAcceptAsync(Consumer<? super T> action) {
        return from(source.thenAcceptAsync(action));
    }

    @Override
    public CompletableFuture<Void> thenAcceptAsync(Consumer<? super T> action, Executor executor) {
        return from(source.thenAcceptAsync(action, executor));
    }

    @Override
    public CompletableFuture<Void> thenRun(Runnable action) {
        return from(source.thenRun(action));
    }

    @Override
    public CompletableFuture<Void> thenRunAsync(Runnable action) {
        return from(source.thenRunAsync(action));
    }

    @Override
    public CompletableFuture<Void> thenRunAsync(Runnable action, Executor executor) {
        return from(source.thenRunAsync(action, executor));
    }

    @Override
    public <U, V> CompletableFuture<V> thenCombine(CompletionStage<? extends U> other, BiFunction<? super T, ? super U, ? extends V> fn) {
        return from(source.thenCombine(other, fn));
    }

    @Override
    public <U, V> CompletableFuture<V> thenCombineAsync(CompletionStage<? extends U> other, BiFunction<? super T, ? super U, ? extends V> fn) {
        return from(source.thenCombineAsync(other, fn));
    }

    @Override
    public <U, V> CompletableFuture<V> thenCombineAsync(CompletionStage<? extends U> other, BiFunction<? super T, ? super U, ? extends V> fn, Executor executor) {
        return from(source.thenCombineAsync(other, fn, executor));
    }

    @Override
    public <U> CompletableFuture<Void> thenAcceptBoth(CompletionStage<? extends U> other, BiConsumer<? super T, ? super U> action) {
        return from(source.thenAcceptBoth(other, action));
    }

    @Override
    public <U> CompletableFuture<Void> thenAcceptBothAsync(CompletionStage<? extends U> other, BiConsumer<? super T, ? super U> action) {
        return from(source.thenAcceptBothAsync(other, action));
    }

    @Override
    public <U> CompletableFuture<Void> thenAcceptBothAsync(CompletionStage<? extends U> other, BiConsumer<? super T, ? super U> action, Executor executor) {
        return from(source.thenAcceptBothAsync(other, action, executor));
    }

    @Override
    public CompletableFuture<Void> runAfterBoth(CompletionStage<?> other, Runnable action) {
        return from(source.runAfterBoth(other, action));
    }

    @Override
    public CompletableFuture<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action) {
        return from(source.runAfterBothAsync(other, action));
    }

    @Override
    public CompletableFuture<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action, Executor executor) {
        return from(source.runAfterBothAsync(other, action, executor));
    }

    @Override
    public <U> CompletableFuture<U> applyToEither(CompletionStage<? extends T> other, Function<? super T, U> fn) {
        return from(source.applyToEither(other, fn));
    }

    @Override
    public <U> CompletableFuture<U> applyToEitherAsync(CompletionStage<? extends T> other, Function<? super T, U> fn) {
        return from(source.applyToEitherAsync(other, fn));
    }

    @Override
    public <U> CompletableFuture<U> applyToEitherAsync(CompletionStage<? extends T> other, Function<? super T, U> fn, Executor executor) {
        return from(source.applyToEitherAsync(other, fn, executor));
    }

    @Override
    public CompletableFuture<Void> acceptEither(CompletionStage<? extends T> other, Consumer<? super T> action) {
        return from(source.acceptEither(other, action));
    }

    @Override
    public CompletableFuture<Void> acceptEitherAsync(CompletionStage<? extends T> other, Consumer<? super T> action) {
        return from(source.acceptEitherAsync(other, action));
    }

    @Override
    public CompletableFuture<Void> acceptEitherAsync(CompletionStage<? extends T> other, Consumer<? super T> action, Executor executor) {
        return from(source.acceptEitherAsync(other, action, executor));
    }

    @Override
    public CompletableFuture<Void> runAfterEither(CompletionStage<?> other, Runnable action) {
        return from(source.runAfterEither(other, action));
    }

    @Override
    public CompletableFuture<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action) {
        return from(source.runAfterEitherAsync(other, action));
    }

    @Override
    public CompletableFuture<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action, Executor executor) {
        return from(source.runAfterEitherAsync(other, action, executor));
    }

    @Override
    public <U> CompletableFuture<U> thenCompose(Function<? super T, ? extends CompletionStage<U>> fn) {
        return from(source.thenCompose(fn));
    }

    @Override
    public <U> CompletableFuture<U> thenComposeAsync(Function<? super T, ? extends CompletionStage<U>> fn) {
        return from(source.thenComposeAsync(fn));
    }

    @Override
    public <U> CompletableFuture<U> thenComposeAsync(Function<? super T, ? extends CompletionStage<U>> fn, Executor executor) {
        return from(source.thenComposeAsync(fn, executor));
    }

    @Override
    public CompletableFuture<T> whenComplete(BiConsumer<? super T, ? super Throwable> action) {
        return from(source.whenComplete(action));
    }

    @Override
    public CompletableFuture<T> whenCompleteAsync(BiConsumer<? super T, ? super Throwable> action) {
        return from(source.whenCompleteAsync(action));
    }

    @Override
    public CompletableFuture<T> whenCompleteAsync(BiConsumer<? super T, ? super Throwable> action, Executor executor) {
        return from(source.whenCompleteAsync(action, executor));
    }

    @Override
    public <U> CompletableFuture<U> handle(BiFunction<? super T, Throwable, ? extends U> fn) {
        return from(source.handle(fn));
    }

    @Override
    public <U> CompletableFuture<U> handleAsync(BiFunction<? super T, Throwable, ? extends U> fn) {
        return from(source.handleAsync(fn));
    }

    @Override
    public <U> CompletableFuture<U> handleAsync(BiFunction<? super T, Throwable, ? extends U> fn, Executor executor) {
        return from(source.handleAsync(fn, executor));
    }

    @Override
    public CompletableFuture<T> toCompletableFuture() {
        return this;
    }

    @Override
    public CompletableFuture<T> exceptionally(Function<Throwable, ? extends T> fn) {
        return from(source.exceptionally(fn));
    }

    @Override
    public boolean isCancelled() {
        return source.isCancelled();
    }

    @Override
    public boolean isCompletedExceptionally() {
        return source.isCompletedExceptionally();
    }

    @Override
    public <U> CompletableFuture<U> newIncompleteFuture() {
        return source.newIncompleteFuture();
    }

    @Override
    public Executor defaultExecutor() {
        return source.defaultExecutor();
    }

    @Override
    public void obtrudeValue(T value) {
        source.obtrudeValue(value);
    }

    @Override
    public void obtrudeException(Throwable ex) {
        source.obtrudeException(ex);
    }

    @Override
    public int getNumberOfDependents() {
        return source.getNumberOfDependents();
    }

    @Override
    public String toString() {
        return source.toString() + " task: " + backingTask.toString();
    }

    @Override
    public CompletableFuture<T> copy() {
        return from(source.copy());
    }

    @Override
    public CompletionStage<T> minimalCompletionStage() {
        return source.minimalCompletionStage();
    }

    @Override
    public CompletableFuture<T> completeAsync(Supplier<? extends T> supplier, Executor executor) {
        return from(source.completeAsync(supplier, executor));
    }

    @Override
    public CompletableFuture<T> completeAsync(Supplier<? extends T> supplier) {
        return from(source.completeAsync(supplier));
    }

    @Override
    public CompletableFuture<T> orTimeout(long timeout, TimeUnit unit) {
        CompletableFuture<T> from = from(source.orTimeout(timeout, unit));
        from.handle((t, throwable) -> backingTask.cancel(true));
        return from;
    }

    @Override
    public CompletableFuture<T> completeOnTimeout(T value, long timeout, TimeUnit unit) {
        CompletableFuture<T> from = from(source.completeOnTimeout(value, timeout, unit));
        from.handle((t, throwable) -> backingTask.cancel(true));
        return from;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return source.cancel(true) && backingTask.cancel(mayInterruptIfRunning);
    }
}
