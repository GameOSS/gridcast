package org.gameoss.gridcast.subscriptions;

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

import org.gameoss.gridcast.p2p.node.NodeId;
import reactor.core.Environment;
import reactor.core.Reactor;
import reactor.core.spec.Reactors;
import reactor.event.Event;
import reactor.event.dispatch.RingBufferDispatcher;
import reactor.function.Consumer;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static reactor.event.selector.Selectors.T;

/**
 * Multi-reader, single writer map of topic to node subscriptions.  It assumes
 * a high number of readers and enforces a async single writer using Reactor's
 * RingBuffer.
 */
public class Subscriptions {

    private final Environment environment;
    private final Reactor reactor;
    private final ConcurrentHashMap<String,TopicInfo> subs;
    private final Executor listenerExecutor;
    private final List<SubscriptionListener> globalListeners = new CopyOnWriteArrayList<>();

    public Subscriptions(int initialCapacity, Executor listenerExecutor) {

        this.listenerExecutor = listenerExecutor;

        // hash table holding subscriptions
        subs = new ConcurrentHashMap<>(initialCapacity,0.75f,Runtime.getRuntime().availableProcessors());

        // ring buffer reactor for write operations
        environment = new Environment();
        reactor = Reactors.reactor()
                .env( environment )
                .dispatcher( new RingBufferDispatcher("subscriptions") )
                .get();

        // register handlers for table operations
        reactor.on( T(AddSubscriber.class), new Consumer<Event<AddSubscriber>>() {
            @Override
            public void accept(Event<AddSubscriber> event) {
                onAddSubscriber(event.getData());
            }
        });
        reactor.on( T(RemoveSubscriber.class), new Consumer<Event<RemoveSubscriber>>() {
            @Override
            public void accept(Event<RemoveSubscriber> event) {
                onRemoveSubscriber(event.getData());
            }
        });
        reactor.on( T(RemoveAllSubscriptions.class), new Consumer<Event<RemoveAllSubscriptions>>() {
            @Override
            public void accept(Event<RemoveAllSubscriptions> event) {
                onRemoveAllSubscriptions(event.getData());
            }
        });
        reactor.on( T(CollectAllSubscriptions.class), new Consumer<Event<CollectAllSubscriptions>>() {
            @Override
            public void accept(Event<CollectAllSubscriptions> event) {
                onCollectAllSubscriptions(event.getData());
            }
        });
        reactor.on( T(AddSubscriptionListener.class), new Consumer<Event<AddSubscriptionListener>>() {
            @Override
            public void accept(Event<AddSubscriptionListener> event) {
                onAddListener(event.getData());
            }
        });
        reactor.on( T(RemoveSubscriptionListener.class), new Consumer<Event<RemoveSubscriptionListener>>() {
            @Override
            public void accept(Event<RemoveSubscriptionListener> event) {
                onRemoveListener(event.getData());
            }
        });
        reactor.on( T(AddUserData.class), new Consumer<Event<AddUserData>>() {
            @Override
            public void accept(Event<AddUserData> event) {
                onAddUserData(event.getData());
            }
        });
        reactor.on( T(RemoveUserData.class), new Consumer<Event<RemoveUserData>>() {
            @Override
            public void accept(Event<RemoveUserData> event) {
                onRemoveUserData(event.getData());
            }
        });
        reactor.on( T(FutureTask.class), new Consumer<Event<FutureTask<?>>>() {
            @Override
            public void accept(Event<FutureTask<?>> event) {
                event.getData().run();
            }
        });
    }


    /**
     * Shutdown the reactor servicing the subscriptions table.
     */
    public void shutdown() {
        environment.shutdown();
    }


    /**
     * Add a global listener that gets called for all subscription changes.
     * The callback occurs on the ring buffer worker thread so the handler
     * MUST BE FAST AND NON-BLOCKING to prevent stalling writes to the
     * subscription table.
     *
     * @param listener
     * @return same listener that was passed into the function.
     */
    public SubscriptionListener addGlobalListener(SubscriptionListener listener) {
        globalListeners.add(listener);
        return listener;
    }


    /**
     * Remove a global listener.
     * @param listener
     */
    public void removeGlobalListener(SubscriptionListener listener) {
        globalListeners.remove(listener);
    }


    /**
     * Add a new subscriber node to a topic.
     *
     * @param topic
     * @param listenerId
     */
    public void addSubscription(String topic, NodeId listenerId) {
        reactor.notify( AddSubscriber.class, Event.wrap(new AddSubscriber(topic, listenerId)) );
    }


    /**
     * Remove a subscriber node from a topic.
     *
     * @param topic
     * @param listenerId
     */
    public void removeSubscription(String topic, NodeId listenerId) {
        reactor.notify( RemoveSubscriber.class, Event.wrap(new RemoveSubscriber(topic,listenerId)) );
    }


    /**
     * Remove all subscriptions for node.
     *
     * @param nodeId
     */
    public void removeAllSubscriptionsForNode(NodeId nodeId) {
        reactor.notify( RemoveAllSubscriptions.class, Event.wrap(new RemoveAllSubscriptions(nodeId)) );
    }


    /**
     * Get all topic subscriptions for node.
     *
     * @param nodeId
     * @param doneListener
     */
    public void collectSubscriptionsForNode(NodeId nodeId, CollectListener doneListener) {
        reactor.notify( CollectAllSubscriptions.class, Event.wrap(new CollectAllSubscriptions(nodeId,doneListener)));
    }


    /**
     * Get the array of subscribers for a topic.
     *
     * @param topic
     * @return list of subscribers or null if none.
     */
    public List<NodeId> getSubscribers(String topic) {
        TopicInfo info = subs.get(topic);
        if (info != null && !info.subscribers.isEmpty()) {
            return info.subscribers;
        } else {
            return null;
        }
    }


    /**
     * Add a new listener to topic subscriptions.
     *
     * @param topic
     * @param listener
     */
    public SubscriptionListener addSubscriptionListener(String topic, SubscriptionListener listener) {
        reactor.notify( AddSubscriptionListener.class, Event.wrap(new AddSubscriptionListener(topic,listener)) );
        return listener;
    }


    /**
     * Remove a listener to topic subscriptions.
     *
     * @param topic
     * @param listener
     */
    public void removeSubscriptionListener(String topic, SubscriptionListener listener) {
        reactor.notify( RemoveSubscriptionListener.class, Event.wrap(new RemoveSubscriptionListener(topic,listener)) );
    }


    /**
     * Associate a userData object with the topic.
     *
     * @param topic
     * @param obj
     * @return
     */
    public Object addUserData(String topic, Object obj) {
        reactor.notify( AddUserData.class, Event.wrap(new AddUserData(topic,obj)) );
        return obj;
    }


    /**
     * Remove association between the object and the topic.
     *
     * @param topic
     * @param obj
     */
    public void removeUserData(String topic, Object obj) {
        reactor.notify( RemoveUserData.class, Event.wrap(new RemoveUserData(topic,obj)));
    }


    /**
     * Get list of user data associated with a topic.
     * @param topic
     * @return list of userData objects or null if none.
     */
    public List<Object> getUserData(String topic) {
        TopicInfo info = subs.get(topic);
        if (info != null && !info.userData.isEmpty()) {
            return info.userData;
        } else {
            return null;
        }
    }


    /**
     * Inserts a fence into the worker queue and returns a future
     * the caller that is completed when then fence has been
     * processed by the writer thread.
     *
     * @return
     */
    public Future<Boolean> addFence() {
        FutureTask<Boolean> futureTask = new FutureTask<>( new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return true;
            }
        });
        reactor.notify( FutureTask.class, Event.wrap(futureTask) );
        return futureTask;
    }


    ///////////////////////////////////////////////////////////////////////////
    // Internals
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Message handler for adding a new subscriber
     *
     * @param msg
     */
    private void onAddSubscriber(final AddSubscriber msg) {
        TopicInfo info = subs.get(msg.topic);

        // add subscriber
        boolean added;
        if (info == null) {
            info = new TopicInfo(msg.listenerId);
            subs.put(msg.topic, info);
            added = true;
        } else {
            added = info.subscribers.addIfAbsent(msg.listenerId);
        }

        // notify listeners if new serverId
        if (added) {
            info.notifyOnSubscribe(msg);
        }
    }


    /**
     * Message handler for removing a subscriber
     * @param msg
     */
    private void onRemoveSubscriber(final RemoveSubscriber msg) {
        TopicInfo info = subs.get(msg.topic);

        // topic not found, early exit
        if (info == null) {
            return;
        }

        // remove server from subscribers and notify listeners
        if (info.subscribers.remove(msg.listenerId)) {
            info.notifyOnUnsubscribe(msg.topic,msg.listenerId);
        }

        // remove topic entry if empty
        if (info.isEmpty()) {
            subs.remove(msg.topic);
        }
    }


    /**
     * Message handler for removing all subscriptions from a node.
     * @param msg
     */
    private void onRemoveAllSubscriptions(final RemoveAllSubscriptions msg) {
        for (Map.Entry<String,TopicInfo> kvp : subs.entrySet()) {
            TopicInfo info = kvp.getValue();

            // remove server from subscribers and notify listeners
            if (info.subscribers.remove(msg.nodeId)) {
                info.notifyOnUnsubscribe(kvp.getKey(),msg.nodeId);
            }

            // remove topic entry if empty
            if (info.isEmpty()) {
                subs.remove(kvp.getKey());
            }
        }
    }


    /**
     * Gather all the topics the node is subscribed.
     * @param msg
     */
    private void onCollectAllSubscriptions(final CollectAllSubscriptions msg) {
        final List<String> topics = new LinkedList<>();
        for (Map.Entry<String,TopicInfo> kvp : subs.entrySet()) {
            TopicInfo info = kvp.getValue();
            if (info.subscribers.contains(msg.nodeId)) {
                topics.add(kvp.getKey());
            }
        }
        listenerExecutor.execute(new Runnable() {
            @Override
            public void run() {
                msg.onDone.onCollectDone(msg.nodeId, topics);
            }
        });
    }


    /**
     * Message handler for adding a subscriber
     * @param msg
     */
    private void onAddListener(final AddSubscriptionListener msg) {
        TopicInfo info = subs.get(msg.topic);

        // add listener
        if (info == null) {
            subs.put(msg.topic, new TopicInfo(msg.listener));
        } else {
            if (info.listeners.addIfAbsent(msg.listener)) {
                // call listener with existing subscribers
                for (NodeId id : info.subscribers) {
                    final NodeId subId = id;
                    listenerExecutor.execute(new Runnable() {
                        @Override
                        public void run() {
                            msg.listener.onSubscribe(msg.topic,subId);
                        }
                    });
                }
            }
        }
    }


    /**
     * Message handler for removing a subscriber
     * @param msg
     */
    private void onRemoveListener(final RemoveSubscriptionListener msg) {
        TopicInfo info = subs.get(msg.topic);

        // topic not found, early exit
        if (info == null) {
            return;
        }

        // remove listener from topic
        info.listeners.remove(msg.listener);

        // remove topic if empty
        if (info.isEmpty()) {
            subs.remove(msg.topic);
        }
    }


    /**
     * Message handler for adding a user object to a topic
     * @param msg
     */
    private void onAddUserData(final AddUserData msg) {
        TopicInfo info = subs.get(msg.topic);

        // add listener
        if (info == null) {
            subs.put(msg.topic, new TopicInfo(msg.userData));
        } else {
            info.userData.addIfAbsent(msg.userData);
        }
    }


    /**
     * Message handler to remove a user object from a topic
     * @param msg
     */
    private void onRemoveUserData(final RemoveUserData msg) {
        TopicInfo info = subs.get(msg.topic);

        // topic not found, early exit
        if (info == null) {
            return;
        }

        // remove listener from topic
        info.userData.remove(msg.userData);

        // remove topic if empty
        if (info.isEmpty()) {
            subs.remove(msg.topic);
        }
    }


    /**
     * Base message representing subscriber events
     */
    private static class SubscriberEvent {
        public final String topic;
        public final NodeId listenerId;

        public SubscriberEvent(String topic, NodeId listenerId) {
            this.topic = topic;
            this.listenerId = listenerId;
        }
    }


    /**
     * Message to add a subscriber
     */
    private static class AddSubscriber extends SubscriberEvent {
        public AddSubscriber(String topic, NodeId listenerId) {
            super(topic, listenerId);
        }
    }


    /**
     * Message to remove a subscriber
     */
    private static class RemoveSubscriber extends SubscriberEvent {
        public RemoveSubscriber(String topic, NodeId listenerId) {
            super(topic,listenerId);
        }
    }


    /**
     * Message to remove all subscriptions from node.
     */
    private static class RemoveAllSubscriptions {
        private final NodeId nodeId;
        public RemoveAllSubscriptions(NodeId listenerId) {
            nodeId = listenerId;
        }
    }

    /**
     * Message to collect all subscriptions for a node.
     */
    private static class CollectAllSubscriptions {
        private final NodeId nodeId;
        private final CollectListener onDone;
        public CollectAllSubscriptions(NodeId nodeId, CollectListener onDone) {
            this.nodeId = nodeId;
            this.onDone = onDone;
        }
    }


    /**
     * Internal message to remove a listener
     */
    private static class AddSubscriptionListener {
        public final String topic;
        public final SubscriptionListener listener;
        public AddSubscriptionListener(String topic, SubscriptionListener listener) {
            this.topic = topic;
            this.listener = listener;
        }
    }


    /**
     * Internal message to remove a listener
     */
    private static class RemoveSubscriptionListener {
        public final String topic;
        public final SubscriptionListener listener;
        public RemoveSubscriptionListener(String topic, SubscriptionListener listener) {
            this.topic = topic;
            this.listener = listener;
        }
    }


    /**
     * Internal message to add a user data object
     */
    private static class AddUserData {
        public final String topic;
        public final Object userData;
        private AddUserData(String topic, Object userData) {
            this.topic = topic;
            this.userData = userData;
        }
    }


    /**
     * Internal message to remove a user data object
     */
    private static class RemoveUserData {
        public final String topic;
        public final Object userData;
        private RemoveUserData(String topic, Object userData) {
            this.topic = topic;
            this.userData = userData;
        }
    }


    /**
     * Internal wrapper for topic subscriptions
     */
    private class TopicInfo {
        public final CopyOnWriteArrayList<NodeId> subscribers = new CopyOnWriteArrayList<>();
        public final CopyOnWriteArrayList<SubscriptionListener> listeners = new CopyOnWriteArrayList<>();
        public final CopyOnWriteArrayList<Object> userData = new CopyOnWriteArrayList<>();

        public TopicInfo(NodeId listenerId) {
            subscribers.add(listenerId);
        }

        public TopicInfo(SubscriptionListener listener) {
            listeners.add(listener);
        }

        public TopicInfo(Object data) {
            userData.add(data);
        }

        public boolean isEmpty() {
            return subscribers.isEmpty() && listeners.isEmpty() && userData.isEmpty();
        }

        public void notifyOnSubscribe(final AddSubscriber msg) {
            // Global listeners are called on the ring buffer thread to reduce
            // overhead.  It does mean that the callback needs to be quick.
            for (SubscriptionListener l : globalListeners) {
                l.onSubscribe(msg.topic, msg.listenerId);
            }

            // Topic listeners are called on the provided executor
            for (SubscriptionListener l : listeners) {
                final SubscriptionListener listener = l;
                listenerExecutor.execute(new Runnable() {
                    @Override
                    public void run() {
                        listener.onSubscribe(msg.topic, msg.listenerId);
                    }
                });
            }
        }

        public void notifyOnUnsubscribe(final String topic, final NodeId listenerId) {
            // Global listeners are called on the ring buffer thread to reduce
            // overhead.  It does mean that the callback needs to be quick.
            for (SubscriptionListener l : globalListeners) {
                l.onUnsubscribe(topic, listenerId);
            }

            // Topic listeners are called on the provided executor
            for (SubscriptionListener l : listeners) {
                final SubscriptionListener listener = l;
                listenerExecutor.execute(new Runnable() {
                    @Override
                    public void run() {
                        listener.onUnsubscribe(topic, listenerId);
                    }
                });
            }
        }
    }
}
