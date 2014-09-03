package org.gameoss.gridcast.p2p.node;

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
import org.gameoss.gridcast.p2p.message.MessageWrapper;

public class LocalNode extends Node {

    public LocalNode(ClusterClient repository, NodeId id, NodeAddress address) {
        super(repository,id,address);
        this.state = State.CONNECTED;
    }

    @Override
    public void sendMessage(MessageWrapper wrapper) {
        cluster.getClusterListener().onMessage(cluster, id, wrapper.getMessage());
    }
}
