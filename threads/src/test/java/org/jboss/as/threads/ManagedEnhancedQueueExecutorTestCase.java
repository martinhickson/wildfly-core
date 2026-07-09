/*
 * Copyright The WildFly Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.jboss.as.threads;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.jboss.msc.service.ServiceController;
import org.jboss.msc.service.StopContext;
import org.jboss.threads.EnhancedQueueExecutor;
import org.junit.After;
import org.junit.Test;

/**
 * Unit tests for {@link ManagedEnhancedQueueExecutor} backed by jboss-threads 3.x {@link EnhancedQueueExecutor}.
 */
public class ManagedEnhancedQueueExecutorTestCase {

    private ManagedEnhancedQueueExecutor executor;

    @After
    public void tearDown() {
        if (executor != null) {
            executor.internalShutdown(new TestStopContext());
        }
    }

    @Test
    public void testNonBlockingExecutorRunsTasks() throws Exception {
        executor = createExecutor(false);
        final CountDownLatch latch = new CountDownLatch(1);

        executor.execute(latch::countDown);

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertFalse(executor.isBlocking());
        assertEquals(4, executor.getMaxThreads());
    }

    @Test
    public void testBlockingExecutorReportsBlockingSemantics() throws Exception {
        executor = createExecutor(true);

        assertTrue(executor.isBlocking());

        final CountDownLatch latch = new CountDownLatch(1);
        executor.execute(latch::countDown);
        assertTrue(latch.await(5, TimeUnit.SECONDS));
    }

    @Test
    public void testThreadPoolMetricsExposeEnhancedQueueExecutor() {
        executor = createExecutor(false);

        assertEquals(2, executor.getCoreThreads());
        assertEquals(4, executor.getMaxThreads());
        assertTrue(executor.getQueueSize() >= 0);
        assertTrue(executor.getCurrentThreadCount() >= 0);
    }

    private static ManagedEnhancedQueueExecutor createExecutor(boolean blocking) {
        EnhancedQueueExecutor enhancedQueueExecutor = new EnhancedQueueExecutor.Builder()
                .setCorePoolSize(2)
                .setMaximumPoolSize(4)
                .setMaximumQueueSize(16)
                .setKeepAliveTime(1, TimeUnit.MINUTES)
                .build();
        return new ManagedEnhancedQueueExecutor(enhancedQueueExecutor, blocking);
    }

    private static final class TestStopContext implements StopContext {
        @Override
        public void asynchronous() {
        }

        @Override
        public void complete() {
        }

        @Override
        public long getElapsedTime() {
            return 0;
        }

        @Override
        public ServiceController<?> getController() {
            return null;
        }

        @Override
        public void execute(Runnable command) {
            command.run();
        }
    }
}
