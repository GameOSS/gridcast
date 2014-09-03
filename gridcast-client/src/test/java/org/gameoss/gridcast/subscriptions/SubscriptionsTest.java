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

        Subscriptions subs = new Subscriptions(1000,executor);
        subs.addSubscription("a", id0);
        subs.addSubscription("a", id0);
        subs.addSubscription("a", id1);
        subs.addFence().get();

        // verify subscriptions have been added
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

        Subscriptions subs = new Subscriptions(1000,executor);
        subs.addSubscription("a", id0);
        subs.addSubscription("a", id1);
        subs.addFence().get();

        // verify subscriptions have been added
        List<NodeId> ids = subs.getSubscribers("a");
        Assert.assertTrue(contains(ids, id0));
        Assert.assertTrue(contains(ids, id1));

        subs.removeSubscription("a",id1);
        subs.removeSubscription("a",id1);
        subs.removeSubscription("a",id1);
        subs.addFence().get();

        // verify subscription has been removed
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

        subs.removeSubscription("a",id1);
        subs.addFence().get();

        // verify subscription has been removed
        List<NodeId> ids = subs.getSubscribers("a");
        Assert.assertNull( ids );

        subs.shutdown();
    }

    @Test
    public void unsubscribeOnlyEntry() throws ExecutionException, InterruptedException {
        NodeId id1 = new NodeId();

        Subscriptions subs = new Subscriptions(1000,executor);

        subs.addSubscription("a",id1);
        subs.removeSubscription("a",id1);
        subs.addFence().get();

        // verify subscription has been removed
        List<NodeId> ids = subs.getSubscribers("a");
        Assert.assertNull( ids );

        subs.shutdown();
    }


    @Test
    public void unsubscribeAll() throws ExecutionException, InterruptedException {
        NodeId id1 = new NodeId();

        Subscriptions subs = new Subscriptions(1000,executor);

        subs.addSubscription("a", id1);
        subs.addSubscription("b", id1);
        subs.addSubscription("c", id1);
        subs.addFence().get();

        Assert.assertTrue( contains(subs.getSubscribers("a"),id1) );
        Assert.assertTrue( contains(subs.getSubscribers("b"),id1) );
        Assert.assertTrue( contains(subs.getSubscribers("c"),id1) );

        subs.removeAllSubscriptionsForNode(id1);
        subs.addFence().get();

        Assert.assertFalse( contains(subs.getSubscribers("a"),id1) );
        Assert.assertFalse( contains(subs.getSubscribers("b"),id1) );
        Assert.assertFalse( contains(subs.getSubscribers("c"),id1) );

        subs.shutdown();
    }


    @Test
    public void listenerAddBefore() throws ExecutionException, InterruptedException {
        final NodeId id1 = new NodeId();

        Subscriptions subs = new Subscriptions(1000,executor);

        final AtomicInteger subCount = new AtomicInteger(0);
        final AtomicInteger unsubCount = new AtomicInteger(0);
        SubscriptionListener listener = subs.addSubscriptionListener("a", new SubscriptionListener() {
            @Override
            public void onSubscribe(String topic, NodeId id) {
                Assert.assertEquals("a", topic);
                Assert.assertEquals(id1, id);
                subCount.incrementAndGet();
            }

            @Override
            public void onUnsubscribe(String topic, NodeId id) {
                Assert.assertEquals("a", topic);
                Assert.assertEquals(id1, id);
                unsubCount.incrementAndGet();
            }
        });

        subs.addSubscription("a",id1);
        subs.removeSubscription("a",id1);
        subs.addFence().get();

        Assert.assertEquals(1, subCount.get());
        Assert.assertEquals(1, unsubCount.get());

        subs.removeSubscriptionListener("a",listener);

        subs.shutdown();
    }


    @Test
    public void basicUserData() throws ExecutionException, InterruptedException {
        final UserData userData = new UserData("My Test Data");

        Subscriptions subs = new Subscriptions(1000,executor);
        subs.addUserData("a", userData );
        subs.addFence().get();

        List<Object> data = subs.getUserData("a");
        Assert.assertEquals(1, data.size());
        Assert.assertEquals(userData, data.get(0));

        subs.removeUserData("a", userData);
        subs.addFence().get();

        Assert.assertNull(subs.getUserData("a"));

        subs.shutdown();
    }


    @Test
    public void globalListenerAndCollect() throws ExecutionException, InterruptedException {
        final NodeId id1 = new NodeId();

        Subscriptions subs = new Subscriptions(1000,executor);

        SubscriptionListener listener = subs.addGlobalListener( new SubscriptionListener() {
            private int state = 0;

            @Override
            public void onSubscribe(String topic, NodeId id) {
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

        subs.addSubscription("a", id1);
        subs.addSubscription("b", id1);
        subs.addSubscription("c", id1);
        subs.addFence().get();

        Assert.assertTrue( contains(subs.getSubscribers("a"),id1) );
        Assert.assertTrue( contains(subs.getSubscribers("b"),id1) );
        Assert.assertTrue( contains(subs.getSubscribers("c"),id1) );


        subs.collectSubscriptionsForNode(id1, new CollectListener() {
            @Override
            public void onCollectDone(NodeId nodeId, List<String> topics) {
                Assert.assertEquals( nodeId, id1);
                Assert.assertTrue( topics.contains("a") );
                Assert.assertTrue( topics.contains("b") );
                Assert.assertTrue( topics.contains("c") );
            }
        });
        subs.addFence().get();

        subs.removeSubscription("a", id1);
        subs.removeSubscription("b", id1);
        subs.removeSubscription("c", id1);
        subs.addFence().get();

        Assert.assertFalse( contains(subs.getSubscribers("a"),id1) );
        Assert.assertFalse( contains(subs.getSubscribers("b"),id1) );
        Assert.assertFalse( contains(subs.getSubscribers("c"),id1) );

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
