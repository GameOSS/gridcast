package org.gameoss.gridcast.message;

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

import java.util.ArrayList;
import java.util.List;


public class AddTopicSubscription {
    private NodeId senderId;
    private List<String> topics;

    public AddTopicSubscription() {
    }

    public AddTopicSubscription(NodeId senderId, String topic) {
        this.senderId = senderId;
        topics = new ArrayList<>();
        topics.add(topic);
    }

    public AddTopicSubscription(NodeId senderId, List<String> topics) {
        this.senderId = senderId;
        this.topics = topics;
    }

    public NodeId getSenderId() {
        return senderId;
    }

    public List<String> getTopics() {
        return topics;
    }
}
