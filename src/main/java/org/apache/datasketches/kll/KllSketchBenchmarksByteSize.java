/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches.kll;

import org.apache.datasketches.common.ArrayOfDoublesSerDe;
import org.apache.datasketches.common.ArrayOfStringsSerDe;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 2, timeUnit = TimeUnit.NANOSECONDS, time = 60000)
@Measurement(iterations = 16, timeUnit = TimeUnit.NANOSECONDS, time = 60000)
public class KllSketchBenchmarksByteSize {

    private static final char[] ALPHABET = "abcdefghijklmnopqrstuvwxyz1234567890".toCharArray();
    private static final int DATA_SIZE = 16384;
    KllFloatsSketch floatsSketch = KllFloatsSketch.newHeapInstance();
    KllDoublesSketch doublesSketch = KllDoublesSketch.newHeapInstance();
    KllItemsSketch<Double> itemsDoublesSketch = KllItemsSketch.newHeapInstance(Double::compareTo, new ArrayOfDoublesSerDe());
    KllItemsSketch<String> itemsSketchString = KllItemsSketch.newHeapInstance(String::compareTo, new ArrayOfStringsSerDe());


    @Setup
    public void setup() {
        IntStream.range(0, DATA_SIZE)
                .map(x -> ThreadLocalRandom.current().nextInt(1024))
                .mapToObj(stringLength -> IntStream.range(0, stringLength)
                        .mapToObj(item -> ThreadLocalRandom.current().nextInt(ALPHABET.length))
                        .map(idx -> Character.toString(ALPHABET[idx]))
                        .collect(Collectors.joining()))
                .forEach(itemsSketchString::update);
        IntStream.range(0, DATA_SIZE)
                .mapToObj(x -> ThreadLocalRandom.current().nextFloat())
                .forEach(floatsSketch::update);
        IntStream.range(0, DATA_SIZE)
                .mapToDouble(x -> ThreadLocalRandom.current().nextDouble())
                .forEach(item -> {
                    doublesSketch.update(item);
                    itemsDoublesSketch.update(item);
                });

    }

    /* getSerializedSizeBytes() */

    @Benchmark
    @OperationsPerInvocation(4096)
    public void getSerializedSizeBytesItemsString() {
        for (int i = 0; i < 4096; i++) {
            itemsSketchString.getSerializedSizeBytes();
        }
    }

    @Benchmark
    @OperationsPerInvocation(4096)
    public void getSerializedSizeBytesItemsDouble() {
        for (int i = 0; i < 4096; i++) {
            itemsDoublesSketch.getSerializedSizeBytes();
        }
    }

    @Benchmark
    @OperationsPerInvocation(4096)
    public void getSerializedSizeBytesRawDouble() {
        for (int i = 0; i < 4096; i++) {
            doublesSketch.getSerializedSizeBytes();
        }
    }

    @Benchmark
    @OperationsPerInvocation(4096)
    public void getSerializedSizeBytesRawFloat() {
        for (int i = 0; i < 4096; i++) {
            floatsSketch.getSerializedSizeBytes();
        }
    }

    /* getTotalItemsNumBytes() */

    @Benchmark
    @OperationsPerInvocation(4096)
    public void getTotalItemsNumBytesItemsString() {
        for (int i = 0; i < 4096; i++) {
            itemsSketchString.getTotalItemsNumBytes();
        }
    }

    @Benchmark
    @OperationsPerInvocation(4096)
    public void getTotalItemsNumBytesItemsDouble() {
        for (int i = 0; i < 4096; i++) {
            itemsDoublesSketch.getTotalItemsNumBytes();
        }
    }

    @Benchmark
    @OperationsPerInvocation(4096)
    public void getTotalItemsNumBytesRawDouble() {
        for (int i = 0; i < 4096; i++) {
            doublesSketch.getTotalItemsNumBytes();
        }
    }

    @Benchmark
    @OperationsPerInvocation(4096)
    public void getTotalItemsNumBytesRawFloat() {
        for (int i = 0; i < 4096; i++) {
            floatsSketch.getTotalItemsNumBytes();
        }
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(KllSketchBenchmarksByteSize.class.getSimpleName())
                .forks(1)
                .resultFormat(ResultFormatType.CSV)
                .build();

        new Runner(opt).run();
    }
}
