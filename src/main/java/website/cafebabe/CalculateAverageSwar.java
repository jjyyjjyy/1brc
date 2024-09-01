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

import sun.misc.Unsafe;

import java.io.File;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * <a href="https://questdb.io/blog/billion-row-challenge-step-by-step/#optimization-4-sunmiscunsafe-swar">SWAR</a>
 */
public class CalculateAverageSwar {

    private static final Unsafe UNSAFE = unsafe();
    private static final String FILE = "./measurements.txt";

    private static Unsafe unsafe() {
        try {
            Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            return (Unsafe) theUnsafe.get(Unsafe.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

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

    static String longToString(long word) {
        final ByteBuffer buf = ByteBuffer.allocate(8).order(ByteOrder.nativeOrder());
        buf.clear();
        buf.putLong(word);
        return new String(buf.array(), StandardCharsets.UTF_8);
    }

    private static class ChunkProcessor implements Runnable {
        private static final int HASHTABLE_SIZE = 4096;
        private static final long BROADCAST_SEMICOLON = 0x3B3B3B3B3B3B3B3BL;
        private static final long BROADCAST_0x01 = 0x0101010101010101L;
        private static final long BROADCAST_0x80 = 0x8080808080808080L;
        private static final long DOT_BITS = 0x10101000;
        private static final long MAGIC_MULTIPLIER = (100 * 0x1000000 + 10 * 0x10000 + 1);
        private final long inputBase;
        private final long inputSize;
        private final StationStats[][] results;
        private final int myIndex;
        private final StatsAcc[] hashtable = new StatsAcc[HASHTABLE_SIZE];

        ChunkProcessor(MemorySegment chunk, StationStats[][] results, int myIndex) {
            this.inputBase = chunk.address();
            this.inputSize = chunk.byteSize();
            this.results = results;
            this.myIndex = myIndex;
        }

        private static long semicolonMatchBits(long word) {
            long diff = word ^ BROADCAST_SEMICOLON;
            return (diff - BROADCAST_0x01) & (~diff & BROADCAST_0x80);
        }

        // credit: artsiomkorzun
        private static long maskWord(long word, long matchBits) {
            long mask = matchBits ^ (matchBits - 1);
            return word & mask;
        }

        // The 4th binary digit of the ascii of a digit is 1 while
        // that of the '.' is 0. This finds the decimal separator.
        // The value can be 12, 20, 28
        private static int dotPos(long word) {
            return Long.numberOfTrailingZeros(~word & DOT_BITS);
        }

        private static int parseTemperature(long numberBytes, int dotPos) {
            // numberBytes contains the number: X.X, -X.X, XX.X or -XX.X
            final long invNumberBytes = ~numberBytes;

            // Calculates the sign
            final long signed = (invNumberBytes << 59) >> 63;
            final int _28MinusDotPos = (dotPos ^ 0b11100);
            final long minusFilter = ~(signed & 0xFF);
            // Use the pre-calculated decimal position to adjust the values
            final long digits = ((numberBytes & minusFilter) << _28MinusDotPos) & 0x0F000F0F00L;

            // Multiply by a magic (100 * 0x1000000 + 10 * 0x10000 + 1), to get the result
            final long absValue = ((digits * MAGIC_MULTIPLIER) >>> 32) & 0x3FF;
            // And apply the sign
            return (int) ((absValue + signed) ^ signed);
        }

        private static int nameLen(long separator) {
            return (Long.numberOfTrailingZeros(separator) >>> 3) + 1;
        }

        private static long hash(long prevHash, long word) {
            return Long.rotateLeft((prevHash ^ word) * 0x51_7c_c1_b7_27_22_0a_95L, 13);
        }

        @Override
        public void run() {
            long cursor = 0;
            while (cursor < inputSize) {
                long nameStartOffset = cursor;
                long hash = 0;
                int nameLen = 0;
                while (true) {
                    long nameWord = UNSAFE.getLong(inputBase + nameStartOffset + nameLen);
                    long matchBits = semicolonMatchBits(nameWord);
                    if (matchBits != 0) {
                        nameLen += nameLen(matchBits);
                        nameWord = maskWord(nameWord, matchBits);
                        hash = hash(hash, nameWord);
                        cursor += nameLen;
                        long tempWord = UNSAFE.getLong(inputBase + cursor);
                        int dotPos = dotPos(tempWord);
                        int temperature = parseTemperature(tempWord, dotPos);
                        cursor += (dotPos >> 3) + 3;
                        findAcc(hash, nameStartOffset, nameLen, nameWord).observe(temperature);
                        break;
                    }
                    hash = hash(hash, nameWord);
                    nameLen += Long.BYTES;
                }
            }
            results[myIndex] = Arrays.stream(hashtable)
                    .filter(Objects::nonNull)
                    .map(StationStats::new)
                    .toArray(StationStats[]::new);
        }

        private StatsAcc findAcc(long hash, long nameStartOffset, int nameLen, long lastNameWord) {
            int initialPos = (int) hash & (HASHTABLE_SIZE - 1);
            int slotPos = initialPos;
            while (true) {
                var acc = hashtable[slotPos];
                if (acc == null) {
                    acc = new StatsAcc(inputBase, hash, nameStartOffset, nameLen, lastNameWord);
                    hashtable[slotPos] = acc;
                    return acc;
                }
                if (acc.hash == hash) {
                    if (acc.nameEquals(inputBase, nameStartOffset, nameLen, lastNameWord)) {
                        return acc;
                    }
                }
                slotPos = (slotPos + 1) & (HASHTABLE_SIZE - 1);
                if (slotPos == initialPos) {
                    throw new RuntimeException(String.format("hash %x, acc.hash %x", hash, acc.hash));
                }
            }
        }

    }

    private static class StatsAcc {
        long[] name;
        long hash;
        int nameLen;
        int sum;
        int count;
        int min;
        int max;

        public StatsAcc(long inputBase, long hash, long nameStartOffset, int nameLen, long lastNameWord) {
            this.hash = hash;
            this.nameLen = nameLen;
            name = new long[(nameLen - 1) / 8 + 1];
            for (int i = 0; i < name.length - 1; i++) {
                name[i] = getLong(inputBase, nameStartOffset + i * Long.BYTES);
            }
            name[name.length - 1] = lastNameWord;
        }

        private static long getLong(long base, long offset) {
            return UNSAFE.getLong(base + offset);
        }

        void observe(int temperature) {
            sum += temperature;
            count++;
            min = Math.min(min, temperature);
            max = Math.max(max, temperature);
        }

        String exportNameString() {
            var buf = ByteBuffer.allocate(name.length * 8).order(ByteOrder.LITTLE_ENDIAN);
            for (long nameWord : name) {
                buf.putLong(nameWord);
            }
            buf.flip();
            final var bytes = new byte[nameLen - 1];
            buf.get(bytes);
            return new String(bytes, StandardCharsets.UTF_8);
        }

        boolean nameEquals(long inputBase, long inputNameStart, long inputNameLen, long lastInputWord) {
            int i = 0;
            for (; i <= inputNameLen - Long.BYTES; i += Long.BYTES) {
                if (getLong(inputBase, inputNameStart + i) != name[i / 8]) {
                    return false;
                }
            }
            return i == inputNameLen || lastInputWord == name[i / 8];
        }
    }

    private static class StationStats implements Comparable<StationStats> {
        private final String name;
        private long sum;
        private int count;
        private int min;
        private int max;

        StationStats(StatsAcc acc) {
            this.name = acc.exportNameString();
            this.sum = acc.sum;
            this.count = acc.count;
            this.min = acc.min;
            this.max = acc.max;
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