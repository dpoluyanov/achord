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

package com.github.mangelion;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;

/**
 * @author Camelion
 * @since 13.03.2018
 * <p>
 * Ensures that there is problem with vtables if many types inherited from one
 * and performance will be slightly better with manual switch case.
 * (Covers only single field type)
 */
@Fork(value = 1, jvmArgs = {"-server"/*, "-XX:+UnlockDiagnosticVMOptions", "-XX:+PrintAssembly"*/})
@BenchmarkMode(AverageTime)
@OutputTimeUnit(NANOSECONDS)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@State(Scope.Benchmark)
public class ColumnTypeBenchmarks {
    private Object[] data;
    private EConsumer eConsumer;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ColumnTypeBenchmarks.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }

    @Setup
    public void setup() {
        eConsumer = EConsumer.valueOf("E_CONSUMER_1");
        data = new Object[]{10000};
    }

    @Benchmark
    public void eConsume(Blackhole bh) {
        eConsumer.consume(data[0], bh);
    }

    @Benchmark
    public void tConsume(Blackhole bh) {
        Types.tconsume((byte) 1, data[0], bh);
    }

    enum EConsumer {
        E_CONSUMER_1 {
            @Override
            void consume(Object o, Blackhole bh) {
                bh.consume((int) o);
            }
        },
        E_CONSUMER_2 {
            @Override
            void consume(Object o, Blackhole bh) {
                bh.consume((int) o);
            }
        }, E_CONSUMER_3 {
            @Override
            void consume(Object o, Blackhole bh) {
                bh.consume((int) o);
            }
        }, E_CONSUMER_4 {
            @Override
            void consume(Object o, Blackhole bh) {
                bh.consume((int) o);
            }
        }, E_CONSUMER_5 {
            @Override
            void consume(Object o, Blackhole bh) {
                bh.consume((int) o);
            }
        }, E_CONSUMER_6 {
            @Override
            void consume(Object o, Blackhole bh) {
                bh.consume((int) o);
            }
        }, E_CONSUMER_7 {
            @Override
            void consume(Object o, Blackhole bh) {
                bh.consume((int) o);
            }
        }, E_CONSUMER_8 {
            @Override
            void consume(Object o, Blackhole bh) {
                bh.consume((int) o);
            }
        };


        abstract void consume(Object o, Blackhole bh);
    }

    static class Types {
        static void tconsume(byte type, Object o, Blackhole bh) {
            switch (type) {
                case 1:
                    consume1(o, bh);
                    return;
                case 2:
                    consume2(o, bh);
                    return;
                case 3:
                    consume3(o, bh);
                    return;
                case 4:
                    consume4(o, bh);
                    return;
                case 5:
                    consume5(o, bh);
                    return;
                case 6:
                    consume6(o, bh);
                    return;
                case 7:
                    consume7(o, bh);
                    return;
                case 8:
                    consume8(o, bh);
                    return;
                default:
                    throw new IllegalArgumentException("Can not write unknown type " + type);
            }
        }

        static void consume1(Object o, Blackhole bh) {
            bh.consume((int) o);
        }

        static void consume2(Object o, Blackhole bh) {
            bh.consume((int) o);
        }

        static void consume3(Object o, Blackhole bh) {
            bh.consume((int) o);
        }

        static void consume4(Object o, Blackhole bh) {
            bh.consume((int) o);
        }

        static void consume5(Object o, Blackhole bh) {
            bh.consume((int) o);
        }

        static void consume6(Object o, Blackhole bh) {
            bh.consume((int) o);
        }

        static void consume7(Object o, Blackhole bh) {
            bh.consume((int) o);
        }

        static void consume8(Object o, Blackhole bh) {
            bh.consume((int) o);
        }
    }
}
