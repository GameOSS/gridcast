package org.gameoss.gridcast.p2p.message;

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

import org.gameoss.gridcast.p2p.serialization.ProtostuffEncoder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Cache serialized byte buffer of the message for cases we need
 * to send the message to multiple nodes.
 */
public class MessageWrapper {
    private static final Logger logger = LoggerFactory.getLogger(MessageWrapper.class);

    private Object message;
    private ByteBuf buf;

    public static MessageWrapper wrap(Object message) {
        return new MessageWrapper(message);
    }

    public MessageWrapper(Object message) {
        this.message = message;
        this.buf = null;
    }

    public Object getMessage() {
        return message;
    }

    public ByteBuf getSerialized(MessageRegistry messageRegistry) {
        if (buf == null) {
            try {
                buf = ProtostuffEncoder.serializeToByteBuf(messageRegistry, Unpooled.directBuffer(256), message);
            } catch (IOException e) {
                logger.error("Exception serializing message", e);
            }
        }
        return buf;
    }

    public void release() {
        if (buf != null) {
            buf.release();
            buf = null;
        }
    }
}
