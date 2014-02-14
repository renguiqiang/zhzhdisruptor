package com.baidu.disruptor;

import com.lmax.disruptor.util.PaddedLong;
import com.lmax.disruptor.util.Util;

import sun.misc.Unsafe;

public class Test {

	private static final Unsafe unsafe = Util.getUnsafe();
	
	public static void main(String[] args) {
		final int base = unsafe.arrayBaseOffset(long[].class);
        final int scale = unsafe.arrayIndexScale(long[].class);
        System.out.println("base" + base);
        System.out.println("scale" + scale);
        System.out.println( base + (scale * 7));
        
        int x = Util.ceilingNextPowerOfTwo(5);
        System.out.println(x);
	}
	
}
