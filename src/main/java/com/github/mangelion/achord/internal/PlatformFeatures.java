/*
 * Copyright 2017-2018 Mangelion
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.mangelion.achord.internal;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

/**
 * @author Dmitriy Poluyanov
 * @since 23.03.2018
 */
final class PlatformFeatures {
    private static final boolean HAS_EPOOL;
    private static final boolean HAS_KQUEUE;

    static {
        MethodType boolType = MethodType.methodType(boolean.class);
        boolean hasEpool;
        try {
            MethodHandle epoolAvailable = MethodHandles.publicLookup()
                    .findStatic(Class.forName("io.netty.channel.epoll.Epool"), "isAvailable", boolType);
            hasEpool = (boolean) epoolAvailable.invoke();
        } catch (Throwable e) {
            hasEpool = false;
        }

        HAS_EPOOL = hasEpool;

        boolean hasKQueue;
        try {
            MethodHandle kqueueAvailable = MethodHandles.publicLookup()
                    .findStatic(Class.forName("io.netty.channel.kqueue.KQueue"), "isAvailable", boolType);
            hasKQueue = (boolean) kqueueAvailable.invoke();
        } catch (Throwable e) {
            hasKQueue = false;
        }

        HAS_KQUEUE = hasKQueue;
    }


    static boolean hasEpool() {
        return HAS_EPOOL;
    }

    static boolean hasKQueue() {
        return HAS_KQUEUE;
    }
}
