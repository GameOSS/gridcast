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

import org.gameoss.gridcast.message.AddTopicSubscription;
import org.gameoss.gridcast.message.RemoveTopicSubscription;
import org.gameoss.gridcast.message.TopicEnvelop;
import org.gameoss.gridcast.p2p.ClusterClient;
import org.gameoss.gridcast.p2p.discovery.NodeDiscovery;
import org.gameoss.gridcast.p2p.listener.ClusterListener;
import org.gameoss.gridcast.p2p.message.MessageRegistry;
import org.gameoss.gridcast.p2p.node.NodeId;
import org.gameoss.gridcast.subscriptions.CollectListener;
import org.gameoss.gridcast.subscriptions.SubscriptionListener;
import org.gameoss.gridcast.subscriptions.Subscriptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.*;

public class GridcastClient implements ClusterListener, SubscriptionListener {
    private static final Logger logger = LoggerFactory.getLogger(GridcastClient.class);

    private static final int USER_MESSAGE_ID_START = MessageRegistry.CUSTOM_START_ID + 128;

    private final ClusterClient cluster;
    private final NodeId localNodeId;
    private final ScheduledExecutorService scheduledExecutorService;
    private final Subscriptions subscriptions;
    private final ConcurrentHashMap<String,RefCntRunnable> subscriptionRefCnt;

    /**
     * Construct a Gridcast client.  Recommended approach is to use {@link GridcastClientBuilder}.
     *
     * @param nodeDiscovery Node discovery provider.
     * @param msNodePollingTime Time in milliseconds to poll for new nodes.
     * @param initialTopicCapacity The initial number of topics.
     * @param listenerExecutor Executor for running listener callbacks.
     */
    public GridcastClient(String hostAddress, int hostPort, NodeDiscovery nodeDiscovery, long msNodePollingTime, int initialTopicCapacity, Executor listenerExecutor) {
        // topic subscription table
        subscriptions = new Subscriptions(initialTopicCapacity, listenerExecutor);
        subscriptions.addGlobalListener(this);
        subscriptionRefCnt = new ConcurrentHashMap<>(initialTopicCapacity,0.75f,Runtime.getRuntime().availableProcessors());

        // p2p cluster
        scheduledExecutorService = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());
        cluster = new ClusterClient(this, hostAddress, hostPort, nodeDiscovery, msNodePollingTime, scheduledExecutorService);
        cluster.getMessageRegistry().addCustomMessage( 1, TopicEnvelop.class);
        cluster.getMessageRegistry().addCustomMessage( 2, AddTopicSubscription.class);
        cluster.getMessageRegistry().addCustomMessage( 3, RemoveTopicSubscription.class);
        localNodeId = cluster.getLocalNodeId();
    }


    /**
     * Terminate client and release its resources.
     */
    public void shutdown() {
        cluster.shutdown();
        subscriptions.shutdown();
        try {
            scheduledExecutorService.shutdownNow();
            scheduledExecutorService.awaitTermination(1,TimeUnit.SECONDS);
        } catch (Exception ex) {
            logger.error("Unexpected exception shutting down scheduledExecutorService", ex);
        }
    }


    /**
     * Register a message for serialization.
     */
    public void registerUserMessage(int id, Class<?> clazz) {
        cluster.getMessageRegistry().addCustomMessage(USER_MESSAGE_ID_START + id, clazz);
    }


    /**
     * Send message to all nodes subscribed to the topic.
     *
     * @param topic
     * @param message
     */
    public void sendMessage(String topic, Object message) {
        List<NodeId> nodes = subscriptions.getSubscribers(topic);
        if (nodes == null || nodes.isEmpty()) {
            return;
        }

        // wrap message in a topic envelop and send to node list
        cluster.sendMessage(nodes, new TopicEnvelop(topic, localNodeId, message));
    }


    /**
     * Send message to ONE randomly selected subscriber to the topic.
     *
     * @param topic
     * @param message
     */
    public void sendMessageToRandom(String topic, Object message) {
        List<NodeId> nodes = subscriptions.getSubscribers(topic);
        if (nodes == null || nodes.isEmpty()) {
            return;
        }

        // send message to randomly selected subscribed node
        NodeId nid = nodes.get( ThreadLocalRandom.current().nextInt(nodes.size()) );
        TopicEnvelop topicEnvelop = new TopicEnvelop(topic, localNodeId, message);
        cluster.sendMessage(nid, topicEnvelop);
    }


    /**
     * Subscribe to messages for a topic.  Callback will occur on the specified executor
     * @param topic
     * @param executor
     */
    public void subscribeToTopic(final String topic, TopicMessageListener listener, Executor executor) {
        // add local node as a subscriber for the topic if this is the first listener
        RefCntRunnable refCnt = subscriptionRefCnt.get(topic);
        if (refCnt == null) {
            RefCntRunnable tmp = new RefCntRunnable(
                    new Runnable() {
                        @Override
                        public void run() {
                            if (subscriptions != null) {
                                subscriptions.addSubscription(topic, localNodeId);
                            }
                        }
                    },
                    new Runnable() {
                        @Override
                        public void run() {
                            if (subscriptions != null) {
                                subscriptions.removeSubscription(topic, localNodeId);
                            }
                        }
            });
            refCnt = subscriptionRefCnt.putIfAbsent(topic,tmp);
            if (refCnt == null) {
                subscriptions.addSubscription(topic, localNodeId);
                refCnt = tmp;
            }
        }
        refCnt.increment();

        // add user data with the message listener
        subscriptions.addUserData(topic, new MessageSubscriberEntry(listener, executor));
    }


    /**
     * Remove message listener for the topic.
     * @param topic
     * @param listener
     * @param executor
     */
    public void unsubscribeToTopic(String topic, TopicMessageListener listener, Executor executor) {
        // remove the user data
        subscriptions.removeUserData(topic, new MessageSubscriberEntry(listener, executor));

        // remove local node subscription for topic if this was the last listener
        subscriptionRefCnt.get(topic).decrement();
    }


    /**
     * Add a fence to subscription table to know when previous operations
     * have finished.
     * @return
     */
    public Future<Boolean> addFence() {
        return subscriptions.addFence();
    }


    ///////////////////////////////////////////////////////////////////////////
    // Internals
    ///////////////////////////////////////////////////////////////////////////

    @Override
    public void onSubscribe(String topic, NodeId id) {
        if (localNodeId.equals(id)) {
            cluster.sendMessageToAll(new AddTopicSubscription(localNodeId,topic), false);
        }
    }

    @Override
    public void onUnsubscribe(String topic, NodeId id) {
        if (localNodeId.equals(id)) {
            cluster.sendMessageToAll(new RemoveTopicSubscription(localNodeId,topic), false);
        }
    }

    @Override
    public void onNodeJoin(ClusterClient cluster, NodeId nodeId) {
        // send local subscriptions to joining node
        if (nodeId != localNodeId) {
            final NodeId targetNodeId = nodeId;
            subscriptions.collectSubscriptionsForNode(localNodeId, new CollectListener() {
                @Override
                public void onCollectDone(NodeId nodeId, List<String> topics) {
                    GridcastClient.this.cluster.sendMessage(targetNodeId, new AddTopicSubscription(localNodeId, topics));
                }
            });
        }
    }

    @Override
    public void onNodeLeft(ClusterClient cluster, NodeId nodeId) {
        subscriptions.removeAllSubscriptionsForNode(nodeId);
    }


    @Override
    public void onMessage(ClusterClient cluster, NodeId senderId, Object message) {
        if (message instanceof TopicEnvelop) {
            // external message to topic
            TopicEnvelop envelop = (TopicEnvelop) message;
            onExternalMessage(envelop.getTopic(), envelop.getSenderId(), envelop.getMessage());
        } else if (message instanceof AddTopicSubscription) {
            // add subscription from remote node
            AddTopicSubscription msg = (AddTopicSubscription) message;
            for (String topic : msg.getTopics()) {
                subscriptions.addSubscription( topic, msg.getSenderId() );
            }
        } else if (message instanceof RemoveTopicSubscription) {
            // remove subscription from remote node
            RemoveTopicSubscription msg = (RemoveTopicSubscription) message;
            for (String topic : msg.getTopics()) {
                subscriptions.removeSubscription(topic, msg.getSenderId());
            }
        }
    }

    private void onExternalMessage(final String topic, final NodeId senderId, final Object message) {
        List<Object> msgSubs = subscriptions.getUserData(topic);
        if (msgSubs != null) {
            for (Object tmp : msgSubs) {
                final MessageSubscriberEntry mse = (MessageSubscriberEntry) tmp;
                mse.getExecutor().execute(new Runnable() {
                    @Override
                    public void run() {
                        mse.getListener().onMessage(topic, senderId, message);
                    }
                });
            }
        }
    }


    private class MessageSubscriberEntry {
        private final TopicMessageListener listener;
        private final Executor executor;

        private MessageSubscriberEntry(TopicMessageListener listener, Executor executor) {
            this.listener = listener;
            this.executor = executor;
        }

        public TopicMessageListener getListener() {
            return listener;
        }

        public Executor getExecutor() {
            return executor;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            MessageSubscriberEntry that = (MessageSubscriberEntry) o;

            if (!executor.equals(that.executor)) return false;
            if (!listener.equals(that.listener)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = listener.hashCode();
            result = 31 * result + executor.hashCode();
            return result;
        }
    }
}
