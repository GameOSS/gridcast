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
import org.gameoss.gridcast.p2p.node.NodeId;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.concurrent.*;

@RunWith(JUnit4.class)
public class BasicClientTest {

    private final Executor executor = new Executor() {
        @Override
        public void execute(Runnable command) {
            command.run();
        }
    };

    @Test
    public void singleNodeMessage() throws InterruptedException, ExecutionException, TimeoutException {
        final String topicA = "MyTopicA";
        final String topicB = "MyTopicB";
        final LinkedBlockingQueue<Object> messagesA = new LinkedBlockingQueue<>();
        final LinkedBlockingQueue<Object> messagesB = new LinkedBlockingQueue<>();

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

        client.registerUserMessage(1, TestMessage.class);

        client.subscribeToTopic(
                topicA,
                new TopicMessageListener() {
                    @Override
                    public void onMessage(String topic, NodeId senderId, Object msg) {
                        messagesA.add(msg);
                    }
                },
                executor);

        client.subscribeToTopic(
                topicB,
                new TopicMessageListener() {
                    @Override
                    public void onMessage(String topic, NodeId senderId, Object msg) {
                        messagesB.add(msg);
                    }
                },
                executor);

        // wait for subscriptions to be registered
        client.addFence().get(1,TimeUnit.SECONDS);

        // send messages to topics
        client.sendMessage(topicA, new TestMessage(topicA));
        client.sendMessage(topicB, new TestMessage(topicB));

        // wait for messages
        TestMessage msgA = (TestMessage) messagesA.poll(1,TimeUnit.SECONDS);
        TestMessage msgB = (TestMessage) messagesB.poll(1,TimeUnit.SECONDS);

        Assert.assertEquals(topicA,msgA.getPayload());
        Assert.assertEquals(topicB,msgB.getPayload());
        Assert.assertTrue(messagesA.isEmpty());
        Assert.assertTrue(messagesB.isEmpty());

        client.shutdown();
    }



    public static class TestMessage {
        private String payload;

        public TestMessage(String payload) {
            this.payload = payload;
        }

        public String getPayload() {
            return payload;
        }
    }

}
