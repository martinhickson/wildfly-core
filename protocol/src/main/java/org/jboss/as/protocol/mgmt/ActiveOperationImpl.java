/*
 * Copyright The WildFly Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.jboss.as.protocol.mgmt;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

import org.jboss.as.protocol.logging.ProtocolLogger;
import org.jboss.remoting3.Channel;
import org.wildfly.common.Assert;
import org.xnio.Cancellable;

/** Standard ActiveOperation implementation */
class ActiveOperationImpl<T, A> implements ActiveOperation<T, A> {

    private static final List<Cancellable> CANCEL_REQUESTED = Collections.emptyList();

    private final A attachment;
    private final Integer operationId;
    private final ResultHandler<T> resultHandler;
    private final CompletableFuture<T> result = new CompletableFuture<>();
    private List<Cancellable> cancellables;
    private volatile Channel channel;

    ActiveOperationImpl(final Integer operationId, final A attachment, final CompletedCallback<T> callback,
                        final AbstractMessageHandler handler) {
        this.operationId = operationId;
        this.attachment = attachment;

        result.whenComplete((value, failure) -> {
            if (failure instanceof CancellationException) {
                handler.removeActiveOperation(operationId);
                callback.cancelled();
                ProtocolLogger.ROOT_LOGGER.debugf("cancelled operation (%d) attachment: (%s) handler: %s.", getOperationId(), getAttachment(), handler);
            } else if (failure != null) {
                if (failure instanceof Exception) {
                    callback.failed((Exception) failure);
                } else {
                    callback.failed(new RuntimeException(failure));
                }
            } else {
                callback.completed(value);
            }
        });

        this.resultHandler = new ResultHandler<>() {
            @Override
            public boolean done(T value) {
                try {
                    return result.complete(value);
                } finally {
                    handler.removeActiveOperation(operationId);
                }
            }

            /**
             * @param t the exception must not be null
             * @return true if the result was successfully set, or false if a result was already set
             */
            @Override
            public boolean failed(final Throwable t) {
                Assert.checkNotNullParam("Throwable", t);
                try {
                    boolean failed = result.completeExceptionally(t);
                    if (failed) {
                        ProtocolLogger.ROOT_LOGGER.debugf(t, "active-op (%d) failed %s", operationId, attachment);
                    }
                    return failed;
                } finally {
                    handler.removeActiveOperation(operationId);
                }
            }

            @Override
            public void cancel() {
                ProtocolLogger.CONNECTION_LOGGER.debugf("Operation (%d) cancelled", operationId);
                ActiveOperationImpl.this.asyncCancel(true);
            }
        };
    }

    @Override
    public Integer getOperationId() {
        return operationId;
    }

    @Override
    public ResultHandler<T> getResultHandler() {
        return resultHandler;
    }

    @Override
    public A getAttachment() {
        return attachment;
    }

    @Override
    public CompletableFuture<T> getResult() {
        return result;
    }

    @Override
    public <U> CompletableFuture<U> getCompletableFuture(Function<T, U> transformer, Consumer<Boolean> asyncCancelTask) {
        return new TransformedCancellableFuture<>(result, transformer,
                asyncCancelTask != null ? asyncCancelTask : this::performAsyncCancel);
    }

    @Override
    public void addCancellable(final Cancellable cancellable) {
        synchronized (this) {
            if (result.isDone()) {
                if (result.isCancelled()) {
                    cancellable.cancel();
                }
                return;
            }
            final List<Cancellable> current = this.cancellables;
            if (current == CANCEL_REQUESTED) {
                cancellable.cancel();
                return;
            }
            (current == null ? (this.cancellables = new ArrayList<>()) : current).add(cancellable);
        }
    }

    void asyncCancel(boolean interruptionDesired) {
        performAsyncCancel(interruptionDesired);
        result.cancel(false);
    }

    private void cancel() {
        asyncCancel(true);
    }

    private void performAsyncCancel(boolean interruptionDesired) {
        final List<Cancellable> toCancel;
        synchronized (this) {
            toCancel = this.cancellables;
            if (toCancel == CANCEL_REQUESTED) {
                return;
            }
            this.cancellables = CANCEL_REQUESTED;
            if (toCancel == null) {
                return;
            }
        }
        for (Cancellable cancellable : toCancel) {
            cancellable.cancel();
        }
    }

    Channel getChannel() {
        return channel;
    }

    void updateChannelRef(Channel channel) {
        if (this.channel == null) {
            this.channel = channel;
        }
    }

    private static final class TransformedCancellableFuture<U, T> extends CompletableFuture<U> {

        private final CompletableFuture<T> source;
        private final AtomicBoolean cancellable = new AtomicBoolean(false);
        private final CountDownLatch latch = new CountDownLatch(1);

        private final Consumer<Boolean> asyncCancelConsumer;

        private TransformedCancellableFuture(CompletableFuture<T> source, Function<T, U> transformer,
                                             Consumer<Boolean> asyncCancelConsumer) {
            this.source = source;
            this.asyncCancelConsumer = asyncCancelConsumer;
            source.whenComplete((value, failure) -> {
                try {
                    if (failure != null) {
                        if (failure instanceof CancellationException) {
                            TransformedCancellableFuture.this.cancel(false);
                        } else {
                            TransformedCancellableFuture.this.completeExceptionally(failure);
                        }
                    } else {
                        TransformedCancellableFuture.this.complete(transformer.apply(value));
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            if (!cancellable.compareAndSet(false, true)) {
                awaitLatch();
                return super.cancel(mayInterruptIfRunning);
            }

            try {
                asyncCancelConsumer.accept(mayInterruptIfRunning);
                if (source.isCancelled()) {
                    ProtocolLogger.ROOT_LOGGER.tracef("%s: source future is cancelled; proceeding to cancel ourself",
                            getClass().getSimpleName());
                } else {
                    ProtocolLogger.ROOT_LOGGER.tracef("%s: Awaiting source future before proceeding to cancel ourself",
                            getClass().getSimpleName());
                    awaitLatch();
                }
                return super.cancel(mayInterruptIfRunning);
            } finally {
                latch.countDown();
            }
        }

        private void awaitLatch() {
            try {
                latch.await();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }
    }

}
