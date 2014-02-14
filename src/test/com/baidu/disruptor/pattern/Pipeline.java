package com.baidu.disruptor.pattern;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.baidu.disruptor.ValueEvent;
import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.SingleThreadedClaimStrategy;
import com.lmax.disruptor.util.Util;


/**
 * 
 * <b>Pipeline a series of messages</b>
 * 
                          +----+    +-----+    +-----+    +-----+
                          | P1 |--->| EP1 |--->| EP2 |--->| EP3 |
                          +----+    +-----+    +-----+    +-----+



                          track to prevent wrap
             +----------------------------------------------------------------+
             |                                                                |
             |                                                                v
+----+    +====+    +=====+    +-----+    +=====+    +-----+    +=====+    +-----+
| P1 |--->| RB |    | SB1 |<---| EP1 |<---| SB2 |<---| EP2 |<---| SB3 |<---| EP3 |
+----+    +====+    +=====+    +-----+    +=====+    +-----+    +=====+    +-----+
     claim   ^  get    |   waitFor           |   waitFor           |  waitFor
             |         |                     |                     |
             +---------+---------------------+---------------------+
 */
public class Pipeline {

	public static void main(String[] args) {

		ExecutorService exec = Executors.newCachedThreadPool();
		
		//RingBuffer
		RingBuffer<ValueEvent> ringBuffer = new RingBuffer<ValueEvent>(new EventFactory<ValueEvent>(){

			@Override
			public ValueEvent newInstance() {
				return new ValueEvent();
			}
			
		}, new SingleThreadedClaimStrategy(4), new BlockingWaitStrategy());

		//SequenceBarrier
		SequenceBarrier barrier1 = ringBuffer.newBarrier();
		//第一个Processor
		BatchEventProcessor<ValueEvent> processor1 = new BatchEventProcessor<ValueEvent>(ringBuffer, barrier1, new EventHandler<ValueEvent>(){

				@Override
				public void onEvent(ValueEvent event, long sequence,
						boolean endOfBatch) throws Exception {
					System.out.println("processor1-" + "Value:" + event.getValue()
							+ ":Thread.id-"
							+ Thread.currentThread().getId());
				}
			
		});

		//协调消费者，与消费者
		SequenceBarrier barrier2 = ringBuffer.newBarrier(Util.getSequencesFor(processor1));
		//第二个Processor
		BatchEventProcessor<ValueEvent> processor2 = new BatchEventProcessor<ValueEvent>(ringBuffer, barrier2, new EventHandler<ValueEvent>(){

			@Override
			public void onEvent(ValueEvent event, long sequence,
					boolean endOfBatch) throws Exception {
				System.out.println("processor2-" + "Value:" + event.getValue()
						+ ":Thread.id-"
						+ Thread.currentThread().getId());
			}
		
		});
		
		//协调消费者，与消费者
		SequenceBarrier barrier3 = ringBuffer.newBarrier(Util.getSequencesFor(processor2));
		//第三个Processor
		BatchEventProcessor<ValueEvent> processor3 = new BatchEventProcessor<ValueEvent>(ringBuffer, barrier3, new EventHandler<ValueEvent>(){

			@Override
			public void onEvent(ValueEvent event, long sequence,
					boolean endOfBatch) throws Exception {
				System.out.println("processor3-" + "Value:" + event.getValue()
						+ ":Thread.id-"
						+ Thread.currentThread().getId());
			}
		
		});

		//协调消费者，与生产者
		ringBuffer.setGatingSequences(Util.getSequencesFor(processor3));

		exec.execute(processor1);
		exec.execute(processor2);
		exec.execute(processor3);

		publish(ringBuffer, 5);
	}
	
	public static void publish(RingBuffer<ValueEvent> ringBuffer, int count) {
		for (int i = 0; i < count; i++) {
			long sequence = ringBuffer.next();
			ValueEvent event = ringBuffer.get(sequence);
			
			event.setValue("number:" + i); // this could be more complex with multiple fields
			System.out.println("Thread.id-" + Thread.currentThread().getId());
			
			// make the event available to EventProcessors
			ringBuffer.publish(sequence);   
		}
	}
	
}
