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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NoOpClusterListener implements ClusterListener {

    private static final Logger logger = LoggerFactory.getLogger(NoOpClusterListener.class);
    
    @Override
    public void onNodeJoin(ClusterClient cluster, NodeId nodeId) {
        logger.debug("Node {} joined", nodeId.toString());
    }

    @Override
    public void onNodeLeft(ClusterClient cluster, NodeId nodeId) {
        logger.debug("Node {} left", nodeId.toString());
    }

    @Override
    public void onMessage(ClusterClient cluster, NodeId senderId, Object message) {
        logger.debug("Message {} received from node {}", message.getClass().toString(), senderId.toString());
    }
}
