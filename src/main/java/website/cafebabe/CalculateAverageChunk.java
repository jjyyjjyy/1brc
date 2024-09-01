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
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <a href="https://questdb.io/blog/billion-row-challenge-step-by-step/#optimization-7-use-smaller-chunks-and-work-stealing">Optimization 7</a>
 */
public class CalculateAverageChunk {
    private static final String FILE = "./measurements.txt";
    private static final int CHUNK_SIZE = 2 * 1024 * 1024;
    private static final AtomicInteger chunkSelector = new AtomicInteger();
    private static final Unsafe UNSAFE = unsafe();
    private static int chunkCount;
    private static StationStats[][] results;
    private static long baseAddress;

    private static Unsafe unsafe() {
        try {
            Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            return (Unsafe) theUnsafe.get(Unsafe.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    static long getLong(long address) {
        return UNSAFE.getLong(address);
    }

    static byte getByte(long address) {
        return UNSAFE.getByte(address);
    }


    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();

        File file = new File(FILE);
        long length = file.length();
        chunkCount = (int) ((length / CHUNK_SIZE - 1) + 1);
        int threadCount = Runtime.getRuntime().availableProcessors();
        results = new StationStats[threadCount][];

        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            final var mappedFile = raf.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, length, Arena.global());
            baseAddress = mappedFile.address();
            var threads = new Thread[threadCount];
            for (int i = 0; i < threadCount; i++) {
                threads[i] = new Thread(new ChunkProcessor(i));
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
        private static final int HASHTABLE_SIZE = 4096;
        private static final long BROADCAST_SEMICOLON = 0x3B3B3B3B3B3B3B3BL;
        private static final long BROADCAST_0x01 = 0x0101010101010101L;
        private static final long BROADCAST_0x80 = 0x8080808080808080L;
        private static final long DOT_BITS = 0x10101000;
        private static final long MAGIC_MULTIPLIER = (100 * 0x1000000 + 10 * 0x10000 + 1);
        private final int myIndex;
        private final StatsAcc[] hashtable = new StatsAcc[HASHTABLE_SIZE];

        ChunkProcessor(int myIndex) {
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

        // Bit 4 of the ascii of a digit is 1, while that of '.' is 0.
        // This finds the decimal separator. The value can be 12, 20, 28.
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
            // And apply sign
            return (int) ((absValue + signed) ^ signed);
        }

        private static int nameLen(long separator) {
            return (Long.numberOfTrailingZeros(separator) >>> 3);
        }

        private static long hash(long word) {
            return Long.rotateLeft(word * 0x51_7c_c1_b7_27_22_0a_95L, 17);
        }

        @Override
        public void run() {
            while (true) {
                var selectedChunk = chunkSelector.getAndIncrement();
                if (selectedChunk >= chunkCount) {
                    break;
                }
                var chunkBase = baseAddress + (long) selectedChunk * CHUNK_SIZE;
                var chunkLimit = baseAddress + (long) (selectedChunk + 1) * CHUNK_SIZE;
                if (selectedChunk > 0) {
                    chunkBase--;
                    while (getByte(chunkBase) != '\n') {
                        chunkBase++;
                    }
                    chunkBase++;
                }
                processChunk(chunkBase, chunkLimit);
            }
            results[myIndex] = Arrays
                    .stream(hashtable)
                    .filter(Objects::nonNull)
                    .map(StationStats::new)
                    .toArray(StationStats[]::new);
        }

        private void processChunk(long chunkBase, long chunkLimit) {
            long cursor = chunkBase;
            long lastNameWord;
            while (cursor < chunkLimit) {
                long nameStartOffset = cursor;
                long nameWord0 = getLong(nameStartOffset);
                long nameWord1 = getLong(nameStartOffset + Long.BYTES);
                long matchBits0 = semicolonMatchBits(nameWord0);
                long matchBits1 = semicolonMatchBits(nameWord1);

                int temperature;
                StatsAcc acc;
                long hash;
                int nameLen;
                if ((matchBits0 | matchBits1) != 0) {
                    int nameLen0 = nameLen(matchBits0);
                    int nameLen1 = nameLen(matchBits1);
                    nameWord0 = maskWord(nameWord0, matchBits0);
                    long nameWord1Mask = (long) nameLen0 << 60 >> 63;
                    nameWord1 = maskWord(nameWord1, matchBits1) & nameWord1Mask;
                    nameLen1 &= (int) (nameWord1Mask & 0b111);
                    nameLen = nameLen0 + nameLen1 + 1;
                    lastNameWord = (nameWord0 & ~nameWord1Mask) | nameWord1;

                    cursor += nameLen;
                    long tempWord = getLong(cursor);
                    int dotPos = dotPos(tempWord);
                    temperature = parseTemperature(tempWord, dotPos);

                    cursor += (dotPos >> 3) + 3;
                    hash = hash(nameWord0);
                    acc = findAcc2(hash, nameWord0, nameWord1);
                    if (acc != null) {
                        acc.observe(temperature);
                        continue;
                    }
                } else {
                    hash = hash(nameWord0);
                    nameLen = 2 * Long.BYTES;
                    while (true) {
                        lastNameWord = getLong(nameStartOffset + nameLen);
                        long matchBits = semicolonMatchBits(lastNameWord);
                        if (matchBits != 0) {
                            nameLen += nameLen(matchBits) + 1;
                            lastNameWord = maskWord(lastNameWord, matchBits);
                            cursor += nameLen;
                            long tempWord = getLong(cursor);
                            int dotPos = dotPos(tempWord);
                            temperature = parseTemperature(tempWord, dotPos);
                            cursor += (dotPos >> 3) + 3;
                            break;
                        }
                        nameLen += Long.BYTES;
                    }
                }
                ensureAcc(hash, nameStartOffset, nameLen, nameWord0, nameWord1, lastNameWord).observe(temperature);
            }
        }

        private StatsAcc findAcc2(long hash, long nameWord0, long nameWord1) {
            int slotPos = (int) hash & (HASHTABLE_SIZE - 1);
            var acc = hashtable[slotPos];
            if (acc != null && acc.hash == hash && acc.nameEquals2(nameWord0, nameWord1)) {
                return acc;
            }
            return null;
        }

        private StatsAcc ensureAcc(
                long hash, long nameStartOffset, int nameLen, long nameWord0, long nameWord1, long lastNameWord) {
            int initialPos = (int) hash & (HASHTABLE_SIZE - 1);
            int slotPos = initialPos;
            while (true) {
                var acc = hashtable[slotPos];
                if (acc == null) {
                    acc = new StatsAcc(hash, nameStartOffset, nameLen, nameWord0, nameWord1, lastNameWord);
                    hashtable[slotPos] = acc;
                    return acc;
                }
                if (acc.hash == hash && acc.nameEquals(nameStartOffset, nameLen, nameWord0, nameWord1, lastNameWord)) {
                    return acc;
                }
                slotPos = (slotPos + 1) & (HASHTABLE_SIZE - 1);
                if (slotPos == initialPos) {
                    throw new RuntimeException(String.format("hash %x, acc.hash %x", hash, acc.hash));
                }
            }
        }
    }

    static class StatsAcc {
        private static final long[] emptyTail = new long[0];
        private static final int NAMETAIL_OFFSET = 2 * Long.BYTES;
        private long nameWord0;
        private long nameWord1;
        private long[] nameTail;
        private long hash;
        private int nameLen;
        private int sum;
        private int count;
        private int min;
        private int max;

        public StatsAcc(long hash, long nameStartOffset, int nameLen, long nameWord0, long nameWord1, long lastNameWord) {
            this.hash = hash;
            this.nameLen = nameLen;
            this.nameWord0 = nameWord0;
            this.nameWord1 = nameWord1;
            int nameTailLen = (nameLen - 1) / 8 - 1;
            if (nameTailLen > 0) {
                nameTail = new long[nameTailLen];
                int i = 0;
                for (; i < nameTailLen - 1; i++) {
                    nameTail[i] = getLong(nameStartOffset + (i + 2L) * Long.BYTES);
                }
                nameTail[i] = lastNameWord;
            } else {
                nameTail = emptyTail;
            }
        }

        boolean nameEquals2(long nameWord0, long nameWord1) {
            return this.nameWord0 == nameWord0 && this.nameWord1 == nameWord1;
        }

        boolean nameEquals(
                long inputNameStart, long inputNameLen, long inputWord0, long inputWord1, long lastInputWord
        ) {
            boolean mismatch0 = inputWord0 != nameWord0;
            boolean mismatch1 = inputWord1 != nameWord1;
            boolean mismatch = mismatch0 | mismatch1;
            if (mismatch | inputNameLen <= NAMETAIL_OFFSET) {
                return !mismatch;
            }
            int i = NAMETAIL_OFFSET;
            for (; i <= inputNameLen - Long.BYTES; i += Long.BYTES) {
                if (getLong(inputNameStart + i) != nameTail[(i - NAMETAIL_OFFSET) / 8]) {
                    return false;
                }
            }
            return i == inputNameLen || lastInputWord == nameTail[(i - NAMETAIL_OFFSET) / 8];
        }

        void observe(int temperature) {
            this.sum += temperature;
            this.count++;
            this.min = Math.min(min, temperature);
            this.max = Math.max(max, temperature);
        }

        String exportNameString() {
            var buf = ByteBuffer.allocate((2 + nameTail.length) * 8).order(ByteOrder.LITTLE_ENDIAN);
            buf.putLong(nameWord0);
            buf.putLong(nameWord1);
            for (long nameWord : nameTail) {
                buf.putLong(nameWord);
            }
            buf.flip();
            final var bytes = new byte[nameLen - 1];
            buf.get(bytes);
            return new String(bytes, StandardCharsets.UTF_8);
        }
    }

    static class StationStats implements Comparable<StationStats> {
        private final String name;
        private long sum;
        private int count;
        private int min;
        private int max;

        StationStats(StatsAcc acc) {
            name = acc.exportNameString();
            sum = acc.sum;
            count = acc.count;
            min = acc.min;
            max = acc.max;
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