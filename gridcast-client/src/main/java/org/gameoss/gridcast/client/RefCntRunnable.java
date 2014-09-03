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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Execute onFirst runnable when reference count goes to 1 and onLast when count goes to 0.
 */
public class RefCntRunnable {
    private static final Logger logger = LoggerFactory.getLogger(RefCntRunnable.class);

    private int count;
    private final Runnable onFirst;
    private final Runnable onLast;

    public RefCntRunnable( Runnable onFirst, Runnable onLast ) {
        this.count = 0;
        this.onFirst = onFirst;
        this.onLast = onLast;
    }

    public synchronized void increment() {
        count += 1;
        if (count == 1 && onFirst != null) {
            onFirst.run();
        }
    }

    public synchronized void decrement() {
        count -= 1;
        if (count == 0 && onLast != null) {
            onLast.run();
        } else if (count < 0) {
            logger.error("Mismatched reference count: {}", count);
        }
    }
}
