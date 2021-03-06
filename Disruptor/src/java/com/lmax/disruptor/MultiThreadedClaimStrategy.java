/*
 * Copyright 2011 LMAX Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.lmax.disruptor;


import java.util.concurrent.atomic.AtomicLongArray;


/**
 * Strategy to be used when there are multiple publisher threads claiming sequences.
 *
 * This strategy is reasonably forgiving when the multiple publisher threads are highly contended or working in an
 * environment where there is insufficient CPUs to handle multiple publisher threads.  It requires 2 CAS operations
 * for a single publisher, compared to the {@link MultiThreadedLowContentionClaimStrategy} strategy which needs only a single
 * CAS and a lazySet per publication.
 */
public final class MultiThreadedClaimStrategy extends AbstractMultithreadedClaimStrategy
    implements ClaimStrategy
{
    private static final int RETRIES = 1000;

    private final AtomicLongArray pendingPublication;
    private final int pendingMask;

    /**
     * Construct a new multi-threaded publisher {@link ClaimStrategy} for a given buffer size.
     *
     * @param bufferSize for the underlying data structure.
     * @param pendingBufferSize number of item that can be pending for serialisation
     */
    public MultiThreadedClaimStrategy(final int bufferSize, final int pendingBufferSize)
    {
        super(bufferSize);
        
        if (Integer.bitCount(pendingBufferSize) != 1)
        {
            throw new IllegalArgumentException("pendingBufferSize must be a power of 2, was: " + pendingBufferSize);
        }

        this.pendingPublication = new AtomicLongArray(pendingBufferSize);
        this.pendingMask = pendingBufferSize - 1;
    }

    /**
     * Construct a new multi-threaded publisher {@link ClaimStrategy} for a given buffer size.
     *
     * @param bufferSize for the underlying data structure.
     */
    public MultiThreadedClaimStrategy(final int bufferSize)
    {
        this(bufferSize, 1024);
    }

    @Override
    public void serialisePublishing(final long sequence, final Sequence cursor, final int batchSize)
    {
        int counter = RETRIES;
        // 如果发布缓冲的长度小于新旧下标之差时，防止缓冲溢出
        while (sequence - cursor.get() > pendingPublication.length())
        {
            if (--counter == 0)
            {
            	// 让出CPU
                Thread.yield();
                counter = RETRIES;
            }
        }

        // 获取旧下标
        long expectedSequence = sequence - batchSize;
        // 批量发布，计算第一个发布位置，除最后一条外，之前调用延迟set，并不一定保证之前的马上能被其他线程读取。性能优化至极。
        for (long pendingSequence = expectedSequence + 1; pendingSequence < sequence; pendingSequence++)
        {
            pendingPublication.lazySet((int) pendingSequence & pendingMask, pendingSequence);
        }
        // 最后一条调用set，根据Volatile语义，将一并将这之上的变量刷新主存，原子
        pendingPublication.set((int) sequence & pendingMask, sequence);

        // 如果当前下标要大于或等于新发布的下标（过程中可能有别的线程在发布），证明下标已经正确设置，可以成功返回。
        long cursorSequence = cursor.get();
        if (cursorSequence >= sequence)
        {
            return;
        }
        
        // 移动下标到下一个位置
        expectedSequence = Math.max(expectedSequence, cursorSequence);
        long nextSequence = expectedSequence + 1;
        // 判断是否是预期的下一个位置，如果不是，说明下个位置一定是别的线程抢占了，退出
        // 如果缓冲是连续的，将一步到位将cursor设置到最新的位置，如果不是也没关系，其他线程也会设置到位
        while (cursor.compareAndSet(expectedSequence, nextSequence))
        {
            expectedSequence = nextSequence;
            nextSequence++;
            if (pendingPublication.get((int) nextSequence & pendingMask) != nextSequence)
            {
                break;
            }
        }
    }
}
