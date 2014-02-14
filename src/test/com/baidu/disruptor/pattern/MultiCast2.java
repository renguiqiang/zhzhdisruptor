package com.baidu.disruptor.pattern;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.baidu.disruptor.ValueEvent;
import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.SingleThreadedClaimStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.util.Util;

/**
 * <b>Multicast a series of messages to multiple EventProcessors</b>
        <pre>
          +-----+                                        track to prevent wrap
   +----->| EP1 |                        +--------------------+----------+----------+
   |      +-----+                        |                    |          |          |
   |                                     |                    v          v          v
+----+    +-----+            +----+    +====+    +====+    +-----+    +-----+    +-----+
| P1 |--->| EP2 |            | P1 |--->| RB |<---| SB |    | EP1 |    | EP2 |    | EP3 |
+----+    +-----+            +----+    +====+    +====+    +-----+    +-----+    +-----+
   |                              claim      get    ^         |          |          |
   |      +-----+                                   |         |          |          |
   +----->| EP3 |                                   +---------+----------+----------+
          +-----+                                                 waitFor
        </pre>
 */
public class MultiCast2 {

public static void main(String[] args) {
		
		ExecutorService exec = Executors.newCachedThreadPool();

		RingBuffer<ValueEvent> ringBuffer = new RingBuffer<ValueEvent>(
				new EventFactory<ValueEvent>() {
					@Override
					public ValueEvent newInstance() {
						return new ValueEvent();
					}
				}, new SingleThreadedClaimStrategy(4),
				new YieldingWaitStrategy());

		// SequenceBarrier
		SequenceBarrier barrier1 = ringBuffer.newBarrier();

		// 第一个EventProcessor
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
		
		//第二个Processor
		BatchEventProcessor<ValueEvent> processor2 = new BatchEventProcessor<ValueEvent>(ringBuffer, barrier1, new EventHandler<ValueEvent>(){

			@Override
			public void onEvent(ValueEvent event, long sequence,
					boolean endOfBatch) throws Exception {
				System.out.println("Value:" + event.getValue()
						+ ":Thread.id-"
						+ Thread.currentThread().getId());
			}
		
		});
		
		//第三个Processor
		BatchEventProcessor<ValueEvent> processor3 = new BatchEventProcessor<ValueEvent>(ringBuffer, barrier1, new EventHandler<ValueEvent>(){

			@Override
			public void onEvent(ValueEvent event, long sequence,
					boolean endOfBatch) throws Exception {
				System.out.println("Value:" + event.getValue()
						+ ":Thread.id-"
						+ Thread.currentThread().getId());
			}
		
		});

		// 提供给生产者参考的消费者track
		ringBuffer.setGatingSequences(Util.getSequencesFor(processor1, processor2, processor3));

		exec.execute(processor1);
		exec.execute(processor2);
		exec.execute(processor3);
		publish(ringBuffer, 20);
	}

	public static void publish(RingBuffer<ValueEvent> ringBuffer, int count) {
		for (int i = 0; i < count; i++) {
			long sequence = ringBuffer.next();
			ValueEvent event = ringBuffer.get(sequence);

			event.setValue("number:" + i); // this could be more complex with
											// multiple fields
			System.out.println("Thread.id-" + Thread.currentThread().getId());

			// make the event available to EventProcessors
			ringBuffer.publish(sequence);
		}
	}
}
