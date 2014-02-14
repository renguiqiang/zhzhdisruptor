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

import com.lmax.disruptor.util.PaddedLong;

import java.util.concurrent.locks.LockSupport;

import static com.lmax.disruptor.util.Util.getMinimumSequence;

/**
 * Optimised strategy can be used when there is a single publisher thread claiming sequences.
 *
 * This strategy must <b>not</b> be used when multiple threads are used for publishing concurrently on the same {@link Sequencer}
 */
public final class SingleThreadedClaimStrategy
    implements ClaimStrategy
{
	// ringbuffer size
    private final int bufferSize;
    // 现在最慢的消费下标
    private final PaddedLong minGatingSequence = new PaddedLong(Sequencer.INITIAL_CURSOR_VALUE);
    // 发布的下标
    private final PaddedLong claimSequence = new PaddedLong(Sequencer.INITIAL_CURSOR_VALUE);

    /**
     * Construct a new single threaded publisher {@link ClaimStrategy} for a given buffer size.
     *
     * @param bufferSize for the underlying data structure.
     */
    public SingleThreadedClaimStrategy(final int bufferSize)
    {
        this.bufferSize = bufferSize;
    }

    @Override
    public int getBufferSize()
    {
        return bufferSize;
    }

    @Override
    public long getSequence()
    {
        return claimSequence.get();
    }

    @Override
    /*
     * 将availableCapacity设为1，与waitForFreeSlotAt同理
     */
    public boolean hasAvailableCapacity(final int availableCapacity, final Sequence[] dependentSequences)
    {
        final long wrapPoint = (claimSequence.get() + availableCapacity) - bufferSize;
        if (wrapPoint > minGatingSequence.get())
        {
            long minSequence = getMinimumSequence(dependentSequences);
            minGatingSequence.set(minSequence);

            if (wrapPoint > minSequence)
            {
                return false;
            }
        }

        return true;
    }

    @Override
    public long incrementAndGet(final Sequence[] dependentSequences)
    {
    	// 单线程无需保证原子
        long nextSequence = claimSequence.get() + 1L;
        claimSequence.set(nextSequence);
        waitForFreeSlotAt(nextSequence, dependentSequences);

        return nextSequence;
    }

    @Override
    public long incrementAndGet(final int delta, final Sequence[] dependentSequences)
    {
    	// 单线程无需保证原子
        long nextSequence = claimSequence.get() + delta;
        claimSequence.set(nextSequence);
        waitForFreeSlotAt(nextSequence, dependentSequences);

        return nextSequence;
    }

    @Override
    public void setSequence(final long sequence, final Sequence[] dependentSequences)
    {
    	// 记录下需要生成的rf序列号
        claimSequence.set(sequence);
        // 将新序号以及现有的消费序列传入，用来判断是否可以publish
        waitForFreeSlotAt(sequence, dependentSequences);
    }

    @Override
    public void serialisePublishing(final long sequence, final Sequence cursor, final int batchSize)
    {
        cursor.set(sequence);
    }
    
    @Override
    public long checkAndIncrement(int availableCapacity, int delta, Sequence[] dependentSequences) 
            throws InsufficientCapacityException
    {
        if (!hasAvailableCapacity(availableCapacity, dependentSequences))
        {
            throw InsufficientCapacityException.INSTANCE;
        }
        
        return incrementAndGet(delta, dependentSequences);
    }

    /**
     * 
     * 	publish 5 times
     *  ringbuffersize 2
     * 
     *  sequence0#wrapPoint-2#minGatingSequence-1#false#minGatingSequence-1
		sequence1#wrapPoint-1#minGatingSequence-1#false#minGatingSequence-1
		sequence2#wrapPoint0#minGatingSequence-1#true#minSequence1#minGatingSequence1
		sequence3#wrapPoint1#minGatingSequence1#false#minGatingSequence1
		sequence4#wrapPoint2#minGatingSequence1#true#minSequence3#minGatingSequence3
     * @param sequence
     * @param dependentSequences
     */
    private void waitForFreeSlotAt(final long sequence, final Sequence[] dependentSequences)
    {
    	// 得到上一轮该位置的序号
        final long wrapPoint = sequence - bufferSize;
        // 如果大于缓存中最小的消费序列，证明缓存无效
//        System.out.print("sequence" + sequence);
//        System.out.print("#wrapPoint" + wrapPoint);
//        System.out.print("#minGatingSequence" + minGatingSequence.get());
//        System.out.print("#" + (wrapPoint > minGatingSequence.get()));
        // 证明已经自旋一圈，需要等待消费
        if (wrapPoint > minGatingSequence.get())
        {
            long minSequence;
            // 消费慢，等待释放slot
            while (wrapPoint > (minSequence = getMinimumSequence(dependentSequences)))
            {
                LockSupport.parkNanos(1L);
            }
//            System.out.print("#minSequence" + minSequence);
            // 设置新的消费位
            minGatingSequence.set(minSequence);
        }
//        System.out.println("#minGatingSequence" + minGatingSequence.get());
    }
}
