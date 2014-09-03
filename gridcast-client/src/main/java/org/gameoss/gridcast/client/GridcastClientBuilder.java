package org.gameoss.gridcast.client;

/*
 * #%L
 * Gridcast
 * %%
 * Copyright (C) 2014 Charles Barry
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */


import org.gameoss.gridcast.p2p.discovery.ConstNodeDiscovery;
import org.gameoss.gridcast.p2p.discovery.NodeDiscovery;

import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class GridcastClientBuilder {

    private String listenHost;
    private int listenPort = -1;
    private NodeDiscovery nodeDiscovery;
    private long nodePollingTime = TimeUnit.SECONDS.toMillis(60);
    private int initialTopicCapacity = 1024;
    private Executor listenerExecutor;


    public static GridcastClientBuilder newBuilder() {
        return new GridcastClientBuilder();
    }

    public GridcastClientBuilder withListenHost(String listenHost) {
        this.listenHost = listenHost;
        return this;
    }

    public GridcastClientBuilder withListenPort(int listenPort) {
        this.listenPort = listenPort;
        return this;
    }

    public GridcastClientBuilder withNodeDiscovery(NodeDiscovery nodeDiscovery) {
        this.nodeDiscovery = nodeDiscovery;
        return this;
    }

    public GridcastClientBuilder withNodePollingTime(long pollingTime, TimeUnit pollingUnit) {
        this.nodePollingTime = pollingUnit.toMillis(pollingTime);
        return this;
    }

    public GridcastClientBuilder withInitialTopicCapacity(int value) {
        this.initialTopicCapacity = value;
        return this;
    }

    public GridcastClientBuilder withListenerExecutor(Executor executor) {
        this.listenerExecutor = executor;
        return this;
    }

    public GridcastClient build() {
        setDefaults();
        return new GridcastClient(listenHost, listenPort, nodeDiscovery, nodePollingTime, initialTopicCapacity, listenerExecutor);
    }

    private void setDefaults() {
        if (listenPort == -1) {
            listenPort = 8000;
        }
        if (nodeDiscovery == null) {
            nodeDiscovery = new ConstNodeDiscovery();
        }
        if (listenerExecutor == null) {
            int maxThreads = Math.max(1, Runtime.getRuntime().availableProcessors() / 4);
            listenerExecutor = new ThreadPoolExecutor(maxThreads, maxThreads, 30, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
        }
    }
}
