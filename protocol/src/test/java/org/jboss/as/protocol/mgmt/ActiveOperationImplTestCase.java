/*
 * Copyright The WildFly Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.jboss.as.protocol.mgmt;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.xnio.Cancellable;

/**
 * Unit tests for {@link ActiveOperationImpl} CompletableFuture-based operation lifecycle.
 */
public class ActiveOperationImplTestCase {

    private TestHandler handler;

    @Before
    public void setUp() {
        handler = new TestHandler();
    }

    @After
    public void tearDown() {
        handler.shutdownNow();
    }

    @Test
    public void testSuccessfulCompletion() throws Exception {
        final CallbackTracker<Integer> tracker = new CallbackTracker<>();
        final ActiveOperation<Integer, String> operation = handler.register("attachment", tracker);

        assertEquals("attachment", operation.getAttachment());
        assertTrue(operation.getResultHandler().done(42));
        assertFalse(operation.getResultHandler().done(99));
        assertEquals(Integer.valueOf(42), operation.getResult().get(1, TimeUnit.SECONDS));

        assertTrue(tracker.completed);
        assertEquals(Integer.valueOf(42), tracker.result);
        assertNull(handler.getActiveOperation(100));
    }

    @Test
    public void testFailure() throws Exception {
        final CallbackTracker<Integer> tracker = new CallbackTracker<>();
        final ActiveOperation<Integer, Void> operation = handler.register(null, tracker);
        final IllegalStateException failure = new IllegalStateException("broken");

        assertTrue(operation.getResultHandler().failed(failure));
        assertFalse(operation.getResultHandler().failed(new RuntimeException("ignored")));

        try {
            operation.getResult().get(1, TimeUnit.SECONDS);
            fail("Expected ExecutionException");
        } catch (ExecutionException e) {
            assertEquals(failure, e.getCause());
        }

        assertTrue(tracker.failed);
        assertEquals(failure, tracker.failure);
    }

    @Test
    public void testCancelViaResultHandlerCancelsRegisteredCancellable() throws Exception {
        final CallbackTracker<Integer> tracker = new CallbackTracker<>();
        final ActiveOperation<Integer, Void> operation = handler.register(null, tracker);
        final AtomicBoolean cancellableInvoked = new AtomicBoolean();

        operation.addCancellable(new Cancellable() {
            @Override
            public Cancellable cancel() {
                cancellableInvoked.set(true);
                return this;
            }
        });

        operation.getResultHandler().cancel();

        assertTrue(operation.getResult().isCancelled());
        assertTrue(cancellableInvoked.get());
        assertTrue(tracker.cancelled);
    }

    @Test
    public void testAddCancellableAfterCancelImmediatelyCancels() {
        final CallbackTracker<Integer> tracker = new CallbackTracker<>();
        final ActiveOperation<Integer, Void> operation = handler.register(null, tracker);
        final AtomicBoolean cancellableInvoked = new AtomicBoolean();

        operation.getResultHandler().cancel();

        operation.addCancellable(new Cancellable() {
            @Override
            public Cancellable cancel() {
                cancellableInvoked.set(true);
                return this;
            }
        });

        assertTrue(cancellableInvoked.get());
    }

    @Test
    public void testGetCompletableFutureTransformsResult() throws Exception {
        final CallbackTracker<Integer> tracker = new CallbackTracker<>();
        final ActiveOperation<Integer, Void> operation = handler.register(null, tracker);

        final CompletableFuture<String> transformed = operation.getCompletableFuture(value -> "value-" + value, null);
        operation.getResultHandler().done(7);

        assertEquals("value-7", transformed.get(1, TimeUnit.SECONDS));
    }

    @Test
    public void testGetCompletableFuturePropagatesSourceCancellation() throws Exception {
        final CallbackTracker<Integer> tracker = new CallbackTracker<>();
        final ActiveOperation<Integer, Void> operation = handler.register(null, tracker);
        final AtomicBoolean cancellableInvoked = new AtomicBoolean();

        operation.addCancellable(new Cancellable() {
            @Override
            public Cancellable cancel() {
                cancellableInvoked.set(true);
                return this;
            }
        });

        final CompletableFuture<String> transformed = operation.getCompletableFuture(value -> "value-" + value, null);
        operation.getResultHandler().cancel();

        assertTrue(operation.getResult().isCancelled());
        assertTrue(transformed.isCancelled());
        assertTrue(cancellableInvoked.get());
        assertTrue(tracker.cancelled);
    }

    @Test
    public void testGetResultRemainsWaitingUntilDone() throws Exception {
        final CallbackTracker<Integer> tracker = new CallbackTracker<>();
        final ActiveOperation<Integer, Void> operation = handler.register(null, tracker);

        try {
            operation.getResult().get(50, TimeUnit.MILLISECONDS);
            fail("Expected TimeoutException");
        } catch (TimeoutException expected) {
            //
        }

        assertFalse(operation.getResult().isDone());
        operation.getResultHandler().done(1);
        assertEquals(Integer.valueOf(1), operation.getResult().get(1, TimeUnit.SECONDS));
    }

    private static final class TestHandler extends AbstractMessageHandler {

        private TestHandler() {
            super(Executors.newSingleThreadExecutor());
        }

        private <T, A> ActiveOperation<T, A> register(A attachment, ActiveOperation.CompletedCallback<T> callback) {
            return registerActiveOperation(100, attachment, callback);
        }
    }

    private static final class CallbackTracker<T> implements ActiveOperation.CompletedCallback<T> {
        private volatile T result;
        private volatile Exception failure;
        private volatile boolean completed;
        private volatile boolean failed;
        private volatile boolean cancelled;

        @Override
        public void completed(T result) {
            this.completed = true;
            this.result = result;
        }

        @Override
        public void failed(Exception e) {
            this.failed = true;
            this.failure = e;
        }

        @Override
        public void cancelled() {
            this.cancelled = true;
        }
    }
}
