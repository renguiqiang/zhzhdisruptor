package com.lmax.disruptor;

import static com.lmax.disruptor.util.Util.getMinimumSequence;

import java.util.concurrent.locks.LockSupport;

import com.lmax.disruptor.util.MutableLong;

public abstract class AbstractMultithreadedClaimStrategy implements ClaimStrategy
{
    private final int bufferSize;
    private final Sequence claimSequence = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);
    private final ThreadLocal<MutableLong> minGatingSequenceThreadLocal = new ThreadLocal<MutableLong>()
    {
        @Override
        protected MutableLong initialValue()
        {
            return new MutableLong(Sequencer.INITIAL_CURSOR_VALUE);
        }
    };

    public AbstractMultithreadedClaimStrategy(int bufferSize)
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
    public boolean hasAvailableCapacity(final int availableCapacity, final Sequence[] dependentSequences)
    {
        return hasAvailableCapacity(claimSequence.get(), availableCapacity, dependentSequences);
    }

    @Override
    public long incrementAndGet(final Sequence[] dependentSequences)
    {
    	// 多线程下需要CAS介入，进行原子加一操作
        final long nextSequence = claimSequence.incrementAndGet();
        waitForFreeSlotAt(nextSequence, dependentSequences, minGatingSequenceThreadLocal.get());
    
        return nextSequence;
    }

    @Override
    public long checkAndIncrement(int availableCapacity, int delta, Sequence[] gatingSequences) throws InsufficientCapacityException
    {
        for (;;)
        {
            long sequence = claimSequence.get();
            if (hasAvailableCapacity(sequence, availableCapacity, gatingSequences))
            {
                long nextSequence = sequence + delta;
                if (claimSequence.compareAndSet(sequence, nextSequence))
                {
                    return nextSequence;
                }
            }
            else
            {
                throw InsufficientCapacityException.INSTANCE;
            }
        }
    }

    @Override
    public long incrementAndGet(final int delta, final Sequence[] dependentSequences)
    {
        final long nextSequence = claimSequence.addAndGet(delta);
        waitForFreeSlotAt(nextSequence, dependentSequences, minGatingSequenceThreadLocal.get());
    
        return nextSequence;
    }

    @Override
    public void setSequence(final long sequence, final Sequence[] dependentSequences)
    {
        claimSequence.set(sequence);
        waitForFreeSlotAt(sequence, dependentSequences, minGatingSequenceThreadLocal.get());
    }

    private void waitForFreeSlotAt(final long sequence, final Sequence[] dependentSequences, final MutableLong minGatingSequence)
    {
    	//取得上轮的该下标序列号
        final long wrapPoint = sequence - bufferSize;
        
        // 判断是否新的位置被消费线程占用
        // 优化，将最慢的消费下标缓存
        if (wrapPoint > minGatingSequence.get())
        {
        	long minSequence;
        	// 真正去遍历sequence寻找最慢的消费下标，真实的值只会比缓存的大。
            while (wrapPoint > (minSequence = getMinimumSequence(dependentSequences)))
            {
            	// 等待直到新的位置被消费
                LockSupport.parkNanos(1L);
            }
    
            minGatingSequence.set(minSequence);
        }
    }

    private boolean hasAvailableCapacity(long sequence, final int availableCapacity, final Sequence[] dependentSequences)
    {
        final long wrapPoint = (sequence + availableCapacity) - bufferSize;
        final MutableLong minGatingSequence = minGatingSequenceThreadLocal.get();
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
}