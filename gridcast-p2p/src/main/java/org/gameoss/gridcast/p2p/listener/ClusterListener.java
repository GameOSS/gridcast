package org.gameoss.gridcast.p2p.listener;

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


import org.gameoss.gridcast.p2p.ClusterClient;
import org.gameoss.gridcast.p2p.node.NodeId;

public interface ClusterListener {

    /**
     * Node has joined the cluster.
     * @param cluster
     * @param nodeId
     */
    public void onNodeJoin(ClusterClient cluster, NodeId nodeId);

    /**
     * Node has left the cluster.
     * @param cluster
     * @param nodeId
     */
    public void onNodeLeft(ClusterClient cluster, NodeId nodeId);

    /**
     * Message received from node.
     * @param cluster
     * @param senderId
     * @param message
     */
    public void onMessage(ClusterClient cluster, NodeId senderId, Object message);
}
