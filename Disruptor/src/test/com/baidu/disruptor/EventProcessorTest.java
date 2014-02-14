package com.baidu.disruptor;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.SingleThreadedClaimStrategy;
import com.lmax.disruptor.util.Util;

/**
 * 每个消息将被所有processor处理
 */
public class EventProcessorTest {

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

		//第二个Processor
		BatchEventProcessor<ValueEvent> processor2 = new BatchEventProcessor<ValueEvent>(ringBuffer, barrier1, new EventHandler<ValueEvent>(){

			@Override
			public void onEvent(ValueEvent event, long sequence,
					boolean endOfBatch) throws Exception {
				System.out.println("processor2-" + "Value:" + event.getValue()
						+ ":Thread.id-"
						+ Thread.currentThread().getId());
			}
		
		});
		
		//协调消费者，与生产者
		ringBuffer.setGatingSequences(Util.getSequencesFor(processor1, processor2));

		exec.execute(processor1);
		exec.execute(processor2);

		publish(ringBuffer, 5);
	}
	
	public static void publish(RingBuffer<ValueEvent> ringBuffer, int count) {
		for (int i = 0; i < count; i++) {
			long sequence = ringBuffer.next();
			ValueEvent event = ringBuffer.get(sequence);
			
			event.setValue("number:" + i); // this could be more complex with multiple fields
			System.out.println("publish-" + Thread.currentThread().getId());
			
			// make the event available to EventProcessors
			ringBuffer.publish(sequence);   
		}
	}
	
}
