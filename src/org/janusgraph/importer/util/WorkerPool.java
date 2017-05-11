package org.janusgraph.importer.util;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class WorkerPool implements AutoCloseable {
	private ThreadPoolExecutor processor;
	private final long shutdownWaitMS = 10000;
	private int maxWorkers;
	private int numThreads;

	public WorkerPool(int numThreads, int maxWorkers) {
		this.numThreads = numThreads;
		this.maxWorkers = maxWorkers;
		initializeNewProcessor(numThreads);
	}
	
	private void initializeNewProcessor(int numThreads) {
		processor = new ThreadPoolExecutor(numThreads, numThreads, 0L, TimeUnit.MILLISECONDS,
				new LinkedBlockingQueue<Runnable>(128));
		processor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
	}

	public void submit(Runnable runnable) throws Exception {
		while ((processor.getTaskCount()-processor.getCompletedTaskCount()) > maxWorkers) {
			Thread.sleep(1000);
		}
		processor.submit(runnable);
	}
	
	private void closeProcessor() throws Exception {
		processor.shutdown();
		while (!processor.awaitTermination(shutdownWaitMS, TimeUnit.MILLISECONDS)) {
		}
		if (!processor.isTerminated()) {
			// log.error("Processor did not terminate in time");
			processor.shutdownNow();
		}
	}

	@Override
	public void close() throws Exception {
		closeProcessor();
	}
}
