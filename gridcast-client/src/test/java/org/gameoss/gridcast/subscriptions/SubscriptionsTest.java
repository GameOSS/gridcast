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
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

@RunWith(JUnit4.class)
public class SubscriptionsTest {

    // in-place executor
    private final Executor executor = new Executor() {
        @Override
        public void execute(Runnable command) {
            command.run();
        }
    };

    @Test
    public void basicSubAndUnsub() throws ExecutionException, InterruptedException {
        NodeId id0 = new NodeId();
        NodeId id1 = new NodeId();
        NodeId id2 = new NodeId();

        // add three nodes to topic "a"
        Subscriptions subs = new Subscriptions(1000,executor);
        subs.addSubscription("a", id0);
        subs.addSubscription("a", id1);
        subs.addSubscription("a", id2);
        subs.addFence().get();

        // verify subscriptions have been added
        List<NodeId> ids = subs.getSubscribers("a");
        Assert.assertEquals( 3, ids.size() );
        Assert.assertTrue(contains(ids, id0) );
        Assert.assertTrue(contains(ids, id1));
        Assert.assertTrue(contains(ids, id2));

        // remove one node from topic "a"
        subs.removeSubscription("a",id1);
        subs.addFence().get();

        // verify subscription has been removed
        ids = subs.getSubscribers("a");
        Assert.assertEquals(2, ids.size());
        Assert.assertTrue(contains(ids, id0));
        Assert.assertFalse(contains(ids, id1));
        Assert.assertTrue(contains(ids, id2));

        subs.shutdown();
    }

    @Test
    public void subscribeDuplicate() throws ExecutionException, InterruptedException {
        NodeId id0 = new NodeId();
        NodeId id1 = new NodeId();

        // add two nodes to topic "a" plus a duplicate node
        Subscriptions subs = new Subscriptions(1000,executor);
        subs.addSubscription("a", id0);
        subs.addSubscription("a", id0);
        subs.addSubscription("a", id1);
        subs.addFence().get();

        // verify only two subscriptions have been added and the duplicate ignored
        List<NodeId> ids = subs.getSubscribers("a");
        Assert.assertEquals( 2, ids.size() );
        Assert.assertTrue(contains(ids, id0) );
        Assert.assertTrue(contains(ids, id1));

        subs.shutdown();
    }

    @Test
    public void unsubscribeDuplicate() throws ExecutionException, InterruptedException {
        NodeId id0 = new NodeId();
        NodeId id1 = new NodeId();

        // add two nodes to topic "a"
        Subscriptions subs = new Subscriptions(1000,executor);
        subs.addSubscription("a", id0);
        subs.addSubscription("a", id1);
        subs.addFence().get();

        // verify subscriptions have been added
        List<NodeId> ids = subs.getSubscribers("a");
        Assert.assertTrue(contains(ids, id0));
        Assert.assertTrue(contains(ids, id1));

        // remove same node 3 times
        subs.removeSubscription("a",id1);
        subs.removeSubscription("a",id1);
        subs.removeSubscription("a",id1);
        subs.addFence().get();

        // verify the node was removed and the duplicate calls ignored
        ids = subs.getSubscribers("a");
        Assert.assertEquals(1,ids.size());
        Assert.assertTrue(contains(ids, id0));
        Assert.assertFalse(contains(ids, id1));

        subs.shutdown();
    }

    @Test
    public void unsubscribeOnEmptyTopic() throws ExecutionException, InterruptedException {
        NodeId id1 = new NodeId();

        Subscriptions subs = new Subscriptions(1000,executor);

        // remove node that was not subscribed to topic "a"
        subs.removeSubscription("a",id1);
        subs.addFence().get();

        // verify subscription list is still empty
        List<NodeId> ids = subs.getSubscribers("a");
        Assert.assertNull( ids );

        subs.shutdown();
    }

    @Test
    public void unsubscribeOnlyEntry() throws ExecutionException, InterruptedException {
        NodeId id = new NodeId();

        Subscriptions subs = new Subscriptions(1000,executor);

        // add and then remove a node to topic "a"
        subs.addSubscription("a",id);
        subs.removeSubscription("a",id);
        subs.addFence().get();

        // verify subscription list is empty
        List<NodeId> ids = subs.getSubscribers("a");
        Assert.assertNull( ids );

        subs.shutdown();
    }


    @Test
    public void unsubscribeAll() throws ExecutionException, InterruptedException {
        NodeId id = new NodeId();

        Subscriptions subs = new Subscriptions(1000,executor);

        // add one node to topics "a", "b", and "c"
        subs.addSubscription("a", id);
        subs.addSubscription("b", id);
        subs.addSubscription("c", id);
        subs.addFence().get();

        // verify node has been added to all three topics
        Assert.assertTrue( contains(subs.getSubscribers("a"),id) );
        Assert.assertTrue( contains(subs.getSubscribers("b"),id) );
        Assert.assertTrue( contains(subs.getSubscribers("c"),id) );

        // bulk remove the node
        subs.removeAllSubscriptionsForNode(id);
        subs.addFence().get();

        // verify all three topics have no subscribers
        Assert.assertFalse( contains(subs.getSubscribers("a"),id) );
        Assert.assertFalse( contains(subs.getSubscribers("b"),id) );
        Assert.assertFalse( contains(subs.getSubscribers("c"),id) );

        subs.shutdown();
    }


    @Test
    public void listenerAddBefore() throws ExecutionException, InterruptedException {
        final NodeId id = new NodeId();

        Subscriptions subs = new Subscriptions(1000,executor);

        // setup a listener for topic subscriptions
        final AtomicInteger subCount = new AtomicInteger(0);
        final AtomicInteger unsubCount = new AtomicInteger(0);
        SubscriptionListener listener = subs.addSubscriptionListener("a", new SubscriptionListener() {
            @Override
            public void onSubscribe(String topic, NodeId id) {
                Assert.assertEquals("a", topic);
                Assert.assertEquals(id, id);
                subCount.incrementAndGet();
            }

            @Override
            public void onUnsubscribe(String topic, NodeId id) {
                Assert.assertEquals("a", topic);
                Assert.assertEquals(id, id);
                unsubCount.incrementAndGet();
            }
        });

        // add and remove a node to a topic for listener to see.
        subs.addSubscription("a",id);
        subs.removeSubscription("a",id);
        subs.addFence().get();

        // verify we got 1 add and 1 remove
        Assert.assertEquals(1, subCount.get());
        Assert.assertEquals(1, unsubCount.get());

        subs.removeSubscriptionListener("a",listener);
        subs.shutdown();
    }


    @Test
    public void listenerAddAfter() throws ExecutionException, InterruptedException {
        final NodeId id = new NodeId();

        Subscriptions subs = new Subscriptions(1000,executor);

        // add node to topic
        subs.addSubscription("a",id);

        // now add a subscriber listener to make sure it gets called with existing nodes
        final AtomicInteger subCount = new AtomicInteger(0);
        final AtomicInteger unsubCount = new AtomicInteger(0);
        SubscriptionListener listener = subs.addSubscriptionListener("a", new SubscriptionListener() {
            @Override
            public void onSubscribe(String topic, NodeId id) {
                Assert.assertEquals("a", topic);
                Assert.assertEquals(id, id);
                subCount.incrementAndGet();
            }

            @Override
            public void onUnsubscribe(String topic, NodeId id) {
                Assert.assertEquals("a", topic);
                Assert.assertEquals(id, id);
                unsubCount.incrementAndGet();
            }
        });

        // remove node from topic
        subs.removeSubscription("a",id);
        subs.addFence().get();

        // verify we got the add and remove
        Assert.assertEquals(1, subCount.get());
        Assert.assertEquals(1, unsubCount.get());

        subs.removeSubscriptionListener("a",listener);
        subs.shutdown();
    }


    @Test
    public void basicUserData() throws ExecutionException, InterruptedException {
        final UserData userData = new UserData("My Test Data");

        Subscriptions subs = new Subscriptions(1000,executor);

        // add user data object to a topic
        subs.addUserData("a", userData );
        subs.addFence().get();

        // retrieve user data from a topic
        List<Object> data = subs.getUserData("a");
        Assert.assertEquals(1, data.size());
        Assert.assertEquals(userData, data.get(0));

        // remove user data from a topic
        subs.removeUserData("a", userData);
        subs.addFence().get();

        Assert.assertNull(subs.getUserData("a"));

        subs.shutdown();
    }


    @Test
    public void globalListenerAndCollect() throws ExecutionException, InterruptedException {
        final NodeId id = new NodeId();

        Subscriptions subs = new Subscriptions(1000,executor);

        // add a global listener for all topic subs / unsubs
        final AtomicInteger subCount = new AtomicInteger(0);
        final AtomicInteger unsubCount = new AtomicInteger(0);
        SubscriptionListener listener = subs.addGlobalListener( new SubscriptionListener() {
            private int state = 0;

            @Override
            public void onSubscribe(String topic, NodeId id) {
                unsubCount.addAndGet(1);
                switch (state) {
                    case 0:
                        Assert.assertEquals("a", topic);
                        state = 1;
                        break;
                    case 1:
                        Assert.assertEquals("b", topic);
                        state = 2;
                        break;
                    case 2:
                        Assert.assertEquals("c", topic);
                        state = 3;
                        break;
                    default:
                        Assert.fail("Invalid state");
                        break;
                }
            }

            @Override
            public void onUnsubscribe(String topic, NodeId id) {
                subCount.addAndGet(1);
                switch (state) {
                    case 3:
                        Assert.assertEquals("a", topic);
                        state = 3;
                        break;
                    case 4:
                        Assert.assertEquals("b", topic);
                        state = 4;
                        break;
                    case 5:
                        Assert.assertEquals("c", topic);
                        state = 5;
                        break;
                    default:
                        Assert.fail("Invalid state");
                        break;
                }
            }
        });

        // add node to topic "a", "b", "c"
        subs.addSubscription("a", id);
        subs.addSubscription("b", id);
        subs.addSubscription("c", id);
        subs.addFence().get();

        // verify node was added
        Assert.assertTrue( contains(subs.getSubscribers("a"),id) );
        Assert.assertTrue( contains(subs.getSubscribers("b"),id) );
        Assert.assertTrue( contains(subs.getSubscribers("c"),id) );

        // queue query for list of topics a node is subscribed
        subs.collectSubscriptionsForNode(id, new CollectListener() {
            @Override
            public void onCollectDone(NodeId nodeId, List<String> topics) {
                Assert.assertEquals( nodeId, id);
                Assert.assertTrue( topics.contains("a") );
                Assert.assertTrue( topics.contains("b") );
                Assert.assertTrue( topics.contains("c") );
            }
        });
        subs.addFence().get();

        // remove node from topics
        subs.removeSubscription("a", id);
        subs.removeSubscription("b", id);
        subs.removeSubscription("c", id);
        subs.addFence().get();

        // verify unsubscribe
        Assert.assertFalse( contains(subs.getSubscribers("a"),id) );
        Assert.assertFalse( contains(subs.getSubscribers("b"),id) );
        Assert.assertFalse( contains(subs.getSubscribers("c"),id) );

        // verify global listener was called for each sub and unsub
        Assert.assertEquals(3, subCount.get());
        Assert.assertEquals(3, unsubCount.get());

        subs.removeGlobalListener(listener);
        subs.shutdown();
    }


    /**
     * Linear search of array for specified value.
     *
     * @param array
     * @param searchValue
     * @return true if the array contains the value, otherwise false.
     */
    private boolean contains(List<NodeId> array, NodeId searchValue) {
        return array != null && array.contains(searchValue);
    }


    /**
     * Test UserData object
     */
    private static class UserData {
        private String data;

        private UserData(String data) {
            this.data = data;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            UserData userData = (UserData) o;

            return data.equals(userData.data);
        }

        @Override
        public int hashCode() {
            return data.hashCode();
        }
    }
}
