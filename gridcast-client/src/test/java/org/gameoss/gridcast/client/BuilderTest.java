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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

@RunWith(JUnit4.class)
public class BuilderTest {
    @Test
    public void defaultBuilder() {
        GridcastClient client = GridcastClientBuilder.newBuilder().build();
        client.shutdown();
    }

    @Test
    public void fullBuilder() throws InterruptedException {
        GridcastClient client = GridcastClientBuilder.newBuilder()
                .withNodeDiscovery(new ConstNodeDiscovery())
                .withNodePollingTime(10, TimeUnit.SECONDS)
                .withInitialTopicCapacity(10)
                .withListenerExecutor(new Executor() {
                    @Override
                    public void execute(Runnable command) {
                        command.run();
                    }
                })
                .build();
        client.shutdown();
    }
}
