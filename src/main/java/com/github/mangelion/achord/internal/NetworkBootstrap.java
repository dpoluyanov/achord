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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

/**
 * @author Dmitriy Poluyanov
 * @since 23.03.2018
 */
public final class NetworkBootstrap {
    private static final MethodHandle epoolGroupCreator;
    private static final MethodHandle kqueueGroupCreator;

    static {
        MethodHandle _epoolGroupCreator, _kqueueGroupCreator;
        MethodType vType = MethodType.methodType(void.class);
        try {
            _epoolGroupCreator = MethodHandles.publicLookup()
                    .findConstructor(Class.forName("io.netty.channel.epool.EpollEventLoopGroup"), vType);
        } catch (ReflectiveOperationException e) {
            _epoolGroupCreator = null;
        }

        epoolGroupCreator = _epoolGroupCreator;

        try {
            _kqueueGroupCreator = MethodHandles.publicLookup()
                    .findConstructor(Class.forName("io.netty.channel.kqueue.KQueueEventLoopGroup"), vType);
        } catch (ReflectiveOperationException e) {
            _kqueueGroupCreator = null;
        }

        kqueueGroupCreator = _kqueueGroupCreator;
    }


    private NetworkBootstrap() { /* restricted */ }

    public static boolean tryNative(Bootstrap b) {
        try {
            if (PlatformFeatures.hasEpool() && epoolGroupCreator != null) {
                b.group((EventLoopGroup) epoolGroupCreator.invoke())
                        .channel((Class<? extends Channel>) Class.forName("io.netty.channel.epoll.EpollSocketChannel"));
                return true;
            } else if (PlatformFeatures.hasKQueue() && kqueueGroupCreator != null) {
                b.group((EventLoopGroup) kqueueGroupCreator.invoke())
                        .channel((Class<? extends Channel>) Class.forName("io.netty.channel.kqueue.KQueueSocketChannel"));
                return true;
            } else {
                return false;
            }
        } catch (Throwable e) {
            return false;
        }
    }
}
