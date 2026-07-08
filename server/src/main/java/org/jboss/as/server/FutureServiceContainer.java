/*
 * Copyright The WildFly Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.jboss.as.server;

import org.jboss.as.server.logging.ServerLogger;
import org.jboss.msc.service.ServiceContainer;

import java.util.concurrent.CompletableFuture;

/**
 * @author John Bailey
 */
public class FutureServiceContainer extends CompletableFuture<ServiceContainer> {

    void done(final ServiceContainer container) {
        complete(container);
    }

    void failed(final Throwable t) {
        Throwable cause = t != null ? t : ServerLogger.ROOT_LOGGER.throwableIsNull();
        completeExceptionally(cause);
    }
}
