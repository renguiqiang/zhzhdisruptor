package com.baidu.disruptor.pattern;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.baidu.disruptor.ValueEvent;
import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.MultiThreadedClaimStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.util.Util;

/**
 * 
 * <b>Sequence a series of messages from multiple publishers</b>
 * 
                                         track to prevent wrap
                                         +--------------------+
                                         |                    |
                                         |                    v
+----+                       +----+    +====+    +====+    +-----+
| P1 |-------+               | P1 |--->| RB |<---| SB |    | EP1 |
+----+       |               +----+    +====+    +====+    +-----+
             v                           ^   get    ^         |
+----+    +-----+            +----+      |          |         |
| P2 |--->| EP1 |            | P2 |------+          +---------+
+----+    +-----+            +----+      |            waitFor
             ^                           |
+----+       |               +----+      |
| P3 |-------+               | P3 |------+
+----+                       +----+
 *
 */

public class MultiCast1 {

	public static void main(String[] args) {
		
		MultiCast1 processor = new MultiCast1();
		
		ExecutorService exec = Executors.newCachedThreadPool();

		RingBuffer<ValueEvent> ringBuffer = new RingBuffer<ValueEvent>(
				new EventFactory<ValueEvent>() {
					@Override
					public ValueEvent newInstance() {
						return new ValueEvent();
					}
				}, new MultiThreadedClaimStrategy(4),
				new BlockingWaitStrategy());

		// SequenceBarrier
		SequenceBarrier barrier1 = ringBuffer.newBarrier();

		// 注册一个EventProcessor
		BatchEventProcessor<ValueEvent> processor1 = new BatchEventProcessor<ValueEvent>(
				ringBuffer, barrier1, new EventHandler<ValueEvent>() {
					@Override
					public void onEvent(ValueEvent event, long sequence,
							boolean endOfBatch) throws Exception {
						System.out.println("Value:" + event.getValue()
								+ ":Thread.id-"
								+ Thread.currentThread().getId());
					}
				});

		// 提供给生产者参考的消费者track
		ringBuffer.setGatingSequences(Util.getSequencesFor(processor1));

		exec.execute(processor1);
		processor.publish(ringBuffer);
	}
	
	public void publish(RingBuffer ringBuffer) {
		Worker worker1 = this.new Worker(ringBuffer, 5);
		new Thread(worker1).start();
		new Thread(worker1).start();
		new Thread(worker1).start();
		new Thread(worker1).start();
		new Thread(worker1).start();
		new Thread(worker1).start();
		new Thread(worker1).start();
	}
	
	class Worker implements Runnable {

		private final RingBuffer<ValueEvent> ringBuffer;
		private final int count;
		
		public Worker(RingBuffer ringBuffer, int count) {
			this.ringBuffer = ringBuffer;
			this.count = count;
		}
		
		@Override
		public void run() {
			for (int j = 0; j < count; j++) {

				long sequence = ringBuffer.next();
				ValueEvent event = ringBuffer.get(sequence);

				event.setValue("number:" + j); 
				System.out.println("Thread.id-"
						+ Thread.currentThread().getId());
				ringBuffer.publish(sequence);
			}
		}
		
	}
	
}

