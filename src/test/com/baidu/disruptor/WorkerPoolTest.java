package com.baidu.disruptor;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.lmax.disruptor.IgnoreExceptionHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.Sequencer;
import com.lmax.disruptor.SingleThreadedClaimStrategy;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.WorkHandler;
import com.lmax.disruptor.WorkerPool;

/**
 * 每一个消息只可能被一个workprocessor消费
 * @author work
 *
 */
public class WorkerPoolTest {

	ExecutorService exec = Executors.newCachedThreadPool();

	public void test() {

		Sequence workSequence = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);
		
		final WorkHandler<ValueEvent> handler = new WorkHandler<ValueEvent>() {
			public void onEvent(final ValueEvent event) throws Exception {
				System.out.println();
				System.out.println("handler:" + event.getValue());
			}
		};
		
		final WorkHandler<ValueEvent> handler1 = new WorkHandler<ValueEvent>() {
			public void onEvent(final ValueEvent event) throws Exception {
				System.out.println();
				System.out.println("handler1:" + event.getValue());
			}
		};

		RingBuffer<ValueEvent> ringBuffer =
		    new RingBuffer<ValueEvent>(ValueEvent.EVENT_FACTORY, 
		                               new SingleThreadedClaimStrategy(1),
		                               new SleepingWaitStrategy());

		//SequenceBarrier
	    SequenceBarrier barrier = ringBuffer.newBarrier(); 
	    
	    WorkerPool<ValueEvent> pool = new WorkerPool<ValueEvent>(ringBuffer, barrier, 
				new IgnoreExceptionHandler(), 
				new WorkHandler[]{handler, handler1});
	    
//		WorkProcessor<ValueEvent> eventProcessor = new WorkProcessor<ValueEvent>(
//				ringBuffer, barrier, handler, 
//				new IgnoreExceptionHandler(), 
//				workSequence);
		ringBuffer.setGatingSequences(pool.getWorkerSequences());  
//
//		// Each EventProcessor can run on a separate thread
//		exec.submit(eventProcessor);
	    
	    pool.start(exec);

		// Publishers claim events in sequence
		for (int i = 0; i < 100; i++) {
			long sequence = ringBuffer.next();
			ValueEvent event = ringBuffer.get(sequence);
			
			event.setValue("number:" + i); // this could be more complex with multiple fields
			
			// make the event available to EventProcessors
			ringBuffer.publish(sequence);   
		}
	}

	public static void main(String[] args) {
		WorkerPoolTest x = new WorkerPoolTest();
		x.test();
	}

}
