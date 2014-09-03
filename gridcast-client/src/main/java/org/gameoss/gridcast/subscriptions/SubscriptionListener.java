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

public interface SubscriptionListener {

    /**
     * Callback when a new subscriber is added to a topic.
     *
     * @param topic name of the topic being subscribed
     * @param id id of the listener
     */
    public void onSubscribe(String topic, NodeId id);

    /**
     * Callback when a subscriber is removed from a topic.
     *
     * @param topic name of the topic being unsubscribed
     * @param id id of the listener unsubscribed
     */
    public void onUnsubscribe(String topic, NodeId id);
}