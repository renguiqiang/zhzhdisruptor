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
package com.lmax.disruptor.util;

/**
 * Cache line padded long variable to be used when false sharing maybe an issue.
 */
/**
 * head 8byte
 * value 8byte
 * padding 6 * 8byte
 * 
 * 这个"可能"解决了false sharing的问题，但不是线程安全的，从它的使用情况能看出来
 * 关于一个64byte的object是否每次都能占据一个cacheline，没找到可信的证据
 * 为什么用volatile，防止重排序
 */
public final class PaddedLong extends MutableLong
{
    public volatile long p1, p2, p3, p4, p5, p6 = 7L;

    /**
     * Default constructor
     */
    public PaddedLong()
    {
    }

    /**
     * Construct with an initial value.
     *
     * @param initialValue for construction
     */
    public PaddedLong(final long initialValue)
    {
        super(initialValue);
    }

    public long sumPaddingToPreventOptimisation()
    {
        return p1 + p2 + p3 + p4 + p5 + p6;
    }
}
