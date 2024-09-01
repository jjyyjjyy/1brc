/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package website.cafebabe;

import java.io.File;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

/**
 * <a href="https://questdb.io/blog/billion-row-challenge-step-by-step/#optimization-2-directly-parse-temperature-as-int">Optimization 2</a>
 */
public class CalculateAverageDoubleParse {

    private static final String FILE = "./measurements.txt";

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();

        File file = new File(FILE);
        long length = file.length();
        int chunkCount = Runtime.getRuntime().availableProcessors();
        StationStats[][] results = new StationStats[chunkCount][];
        long[] chunkOffsets = new long[chunkCount];

        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            for (int i = 0; i < chunkOffsets.length; i++) {
                long start = length * i / chunkCount;
                raf.seek(start);
                while (raf.read() != (byte) '\n') {
                }
                start = raf.getFilePointer();
                chunkOffsets[i] = start;
            }
            MemorySegment mappedFile = raf.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, length, Arena.global());
            Thread[] threads = new Thread[chunkCount];
            for (int i = 0; i < chunkCount; i++) {
                long start = chunkOffsets[i];
                long limit = ((i + 1) < chunkCount) ? chunkOffsets[i + 1] : length;
                threads[i] = new Thread(new ChunkProcessor(mappedFile.asSlice(start, limit - start), results, i));
            }

            for (Thread thread : threads) {
                thread.start();
            }
            for (Thread thread : threads) {
                thread.join();
            }
            mappedFile.unload();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        Map<String, StationStats> measurements = new TreeMap<>();
        for (StationStats[] result : results) {
            for (StationStats stationStats : result) {
                measurements.merge(stationStats.name, stationStats, (old, current) -> {
                    old.count += current.count;
                    old.sum += current.sum;
                    old.min = Math.min(current.min, old.min);
                    old.max = Math.max(current.max, old.max);
                    return old;
                });
            }
        }

        long cost = System.currentTimeMillis() - startTime;
        System.out.println("Cost: " + cost + "ms.");
        System.out.println(measurements);
    }

    private static class ChunkProcessor implements Runnable {
        private final MemorySegment chunk;
        private final StationStats[][] results;
        private final int myIndex;
        private final Map<String, StationStats> statsMap = new HashMap<>();

        ChunkProcessor(MemorySegment chunk, StationStats[][] results, int myIndex) {
            this.chunk = chunk;
            this.results = results;
            this.myIndex = myIndex;
        }

        @Override
        public void run() {
            for (var cursor = 0L; cursor < chunk.byteSize(); ) {
                long semicolonPos = findByte(cursor, ';');
                long newlinePos = findByte(semicolonPos + 1, '\n');
                int intTemp = parseTemperature(semicolonPos);

                String name = stringAt(cursor, semicolonPos);
                StationStats stats = statsMap.computeIfAbsent(name, k -> new StationStats(name));
                stats.sum += intTemp;
                stats.count++;
                stats.min = Math.min(stats.min, intTemp);
                stats.max = Math.max(stats.max, intTemp);
                cursor = newlinePos + 1;
            }
            results[myIndex] = statsMap.values().toArray(StationStats[]::new);
        }

        private int parseTemperature(long semicolonPos) {
            long off = semicolonPos + 1;
            int sign = 1;
            byte b = chunk.get(JAVA_BYTE, off++);
            if (b == '-') {
                sign = -1;
                b = chunk.get(JAVA_BYTE, off++);
            }
            int temp = b - '0';
            b = chunk.get(JAVA_BYTE, off++);
            if (b != '.') {
                temp = 10 * temp + b - '0';
                // we found two integer digits. The next char is definitely '.', skip it:
                off++;
            }
            b = chunk.get(JAVA_BYTE, off);
            temp = 10 * temp + b - '0';
            return sign * temp;
        }

        private long findByte(long cursor, int b) {
            for (var i = cursor; i < chunk.byteSize(); i++) {
                if (chunk.get(JAVA_BYTE, i) == b) {
                    return i;
                }
            }
            throw new RuntimeException(((char) b) + " not found");
        }


        private String stringAt(long start, long limit) {
            return new String(
                    chunk.asSlice(start, limit - start).toArray(JAVA_BYTE),
                    StandardCharsets.UTF_8
            );
        }
    }


    private static class StationStats implements Comparable<StationStats> {
        private final String name;
        private long sum;
        private int count;
        private int min;
        private int max;

        public StationStats(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return String.format("%.1f/%.1f/%.1f", min / 10.0, Math.round((double) sum / count) / 10.0, max / 10.0);
        }

        @Override
        public boolean equals(Object that) {
            return that.getClass() == StationStats.class && ((StationStats) that).name.equals(this.name);
        }

        @Override
        public int compareTo(StationStats that) {
            return name.compareTo(that.name);
        }
    }
}