/*
 * Copyright 2012 LMAX Ltd.
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

import com.lmax.disruptor.util.Util;

import sun.misc.Unsafe;

/*
 * 用于跟踪ringBuffer的进度和事件处理器的进度
 * 
 * 为什么不去实现一个并发安全的PaddedLong,而要用这个呢？
 * "It seems Java 7 got clever and eliminated or re-ordered the unused fields, thus re-introducing false sharing. "
 * 
 * 这里为什么用数组，而且长度是15，而且起始的更新位置是72
 * 
 * 个人理解
 * 1. paddedLong线程不安全
 *	  TODO: java对象是否cache line对齐
 *		也就是说有可能一半在上一个cacheline，另一半在下一个cacheline，这样有false share的问题发生。
 * 2. 数组是一段连续内存，假如加载其中的一个元素，那整个cacheline就很有可能加载到本地cache中。
 * 3. 一个数组的内存构成是：头部8位+数组长度8位+数组元素8*N
 * 		 头部       8
		 长度       16
		 1     24
		 2	   32 	
		 3     40 
		 4     48
		 5     56
		 6     64
		 7     72
		 [8]   80 
		 9     88
		 10    96 
		 11    104
		 12    112
		 13    120 
		 14    128
		 15    132
	   这样就保证了第八个元素永远被一个cacheline独享，避免false share的问题
 * 	
 */
public class Sequence
{
    private static final Unsafe unsafe;
    private static final long valueOffset;

    static
    {
        unsafe = Util.getUnsafe();
        // 获取数组的头部大小，16
        final int base = unsafe.arrayBaseOffset(long[].class);
        // 获取元素占位大小，8
        final int scale = unsafe.arrayIndexScale(long[].class);
        // 72位
        valueOffset = base + (scale * 7);
    }

    private final long[] paddedValue = new long[15];

    public Sequence()
    {
        setOrdered(-1);
    }

    public Sequence(final long initialValue)
    {
        setOrdered(initialValue);
    }

    public long get()
    {
    	// 获取第八个元素
        return unsafe.getLongVolatile(paddedValue, valueOffset);
    }

    public void set(final long value)
    {
    	// 在数组内找到偏移量offset，设置为value，换句话说在这里将value放入数组第八位
    	// volatile set
        unsafe.putOrderedLong(paddedValue, valueOffset, value);
    }

    private void setOrdered(final long value)
    {
    	// 设置下标初始值-1
        unsafe.putOrderedLong(paddedValue, valueOffset, value);
    }

    public boolean compareAndSet(final long expectedValue, final long newValue)
    {
    	// 只围绕第八个元素进行运算
        return unsafe.compareAndSwapLong(paddedValue, valueOffset, expectedValue, newValue);
    }

    public String toString()
    {
        return Long.toString(get());
    }
    
    public long incrementAndGet()
    {
        return addAndGet(1L);
    }

    public long addAndGet(final long increment)
    {
        long currentValue;
        long newValue;

        do
        {
            currentValue = get();
            newValue = currentValue + increment;
        }
        while (!compareAndSet(currentValue, newValue));

        return newValue;
    }
}

