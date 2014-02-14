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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.lmax.disruptor.util.Util.getMinimumSequence;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Blocking strategy that uses a lock and condition variable for {@link EventProcessor}s waiting on a barrier.
 *
 * This strategy can be used when throughput and low-latency are not as important as CPU resource.
 * 使用锁和条件变量。CPU资源的占用少，延迟大。
 */
public final class BlockingWaitStrategy implements WaitStrategy
{
    private final Lock lock = new ReentrantLock();
    private final Condition processorNotifyCondition = lock.newCondition();
    private volatile int numWaiters = 0;

    @Override
    public long waitFor(final long sequence, final Sequence cursor, final Sequence[] dependents, final SequenceBarrier barrier)
        throws AlertException, InterruptedException
    {
        long availableSequence;
        // 现有的发布下标比新请求的小，证明发布要比消费慢，要等待新的发布才能继续进行
        if ((availableSequence = cursor.get()) < sequence)
        {
            lock.lock();
            try
            {
                ++numWaiters;
                while ((availableSequence = cursor.get()) < sequence)
                {
                    barrier.checkAlert();
                    // block中，等待唤醒
                    processorNotifyCondition.await(1, MILLISECONDS);
                }
            }
            finally
            {
                --numWaiters;
                lock.unlock();
            }
        }

        // 让有上游时
        if (0 != dependents.length)
        {
        	// 当依赖消费者已消费最小序号大于等于当前消费的序号时，证明上游都跑完了，赶紧返回。否则，等待上游消费完。
            while ((availableSequence = getMinimumSequence(dependents)) < sequence)
            {
                barrier.checkAlert();
            }
        }

        return availableSequence;
    }

    // 可配置等待超时时间的waitFor版本
    @Override
    public long waitFor(final long sequence, final Sequence cursor, final Sequence[] dependents, final SequenceBarrier barrier,
                        final long timeout, final TimeUnit sourceUnit)
        throws AlertException, InterruptedException
    {
        long availableSequence;
        if ((availableSequence = cursor.get()) < sequence)
        {
            lock.lock();
            try
            {
                ++numWaiters;
                while ((availableSequence = cursor.get()) < sequence)
                {
                    barrier.checkAlert();

                    if (!processorNotifyCondition.await(timeout, sourceUnit))
                    {
                        break;
                    }
                }
            }
            finally
            {
                --numWaiters;
                lock.unlock();
            }
        }

        if (0 != dependents.length)
        {
            while ((availableSequence = getMinimumSequence(dependents)) < sequence)
            {
                barrier.checkAlert();
            }
        }

        return availableSequence;
    }

    @Override
    public void signalAllWhenBlocking()
    {
        if (0 != numWaiters)
        {
            lock.lock();
            try
            {
                processorNotifyCondition.signalAll();
            }
            finally
            {
                lock.unlock();
            }
        }
    }
}
