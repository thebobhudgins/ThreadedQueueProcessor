package QueueProcess;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class AbstractFacilitator<T> {
	
	private ExecutorService _executor;
	
	protected ConcurrentLinkedQueue<T> queue = new ConcurrentLinkedQueue<>();
	private Collection<Throwable> errors = new ArrayList<>();
	
	protected AtomicInteger processed = new AtomicInteger();
	protected AtomicInteger totalItems = new AtomicInteger();
	
	private Future<Object> facilitatorFuture;
	private Collection<Future<Object>> processorsFuture = new ArrayList<>();
	private ICallback _callback;
	private IProgressListener _progressListener;
	
	private int queueSizeLimit = 20;
	private int maxThreads = 5; // Configuration.getInt("api.maxthreads");
	
	public AbstractFacilitator (ExecutorService executor, ICallback callback, IProgressListener progressListener) {
		
		_executor = executor;
		_callback = callback;
		_progressListener = progressListener;
	}
	
	public HashMap<String, String> getDetails() {
		
		HashMap<String, String> details = new HashMap<>();
		details.put("threads", processorsFuture.size() + " / " + maxThreads);
		details.put("queue", queue.size() + " / " + queueSizeLimit);
		
		return details;
	}
	
	protected abstract AbstractWorker<T> createQueueWorker();
	
	private void incrementProcessed(int size) {
		processed.addAndGet(size);
		progressed();
	}

	protected void itemsDone(Collection<T> items) {
		incrementProcessed(items.size());
	}
	
	protected void itemsError(Collection<T> items, Throwable err) {
		err.printStackTrace();
		incrementProcessed(items.size());
	}
	
	public void addToQueue(T item) {
		
		if (queueSizeLimit > 0) {
			while (queue.size() >= queueSizeLimit) {
				try { TimeUnit.MILLISECONDS.sleep(500); }
				catch (Exception exc) { /* JLog.printStackTrace(exc); */ }
			}
		}
		
		queue.add(item);
		
		totalItems.incrementAndGet();
		progressed();
		runFacilitator();
	}
	
	private synchronized void runFacilitator() {
		
		if (facilitatorFuture != null
				&& !facilitatorFuture.isDone())
			return;
		
		facilitatorFuture = _executor
				.submit(new Callable<Object>() {

			@Override
			public Object call() throws Exception {

				while (!isDone()) {
					
					while (!queue.isEmpty()) {
						
						validateFutures();
						
						if (processorsFuture.size() < maxThreads) {
							processorsFuture.add(_executor.submit(createQueueWorker()));
						}
					}
					
					try { TimeUnit.MILLISECONDS.sleep(200); }
					catch (Exception exc) { return null; }
				}
				
				if (_callback != null)
					_callback.done(getErrors());
					
				return null;
			}
			
		});
	}
	
	public Collection<Throwable> getErrors() {
		return errors;
	}
	
	public boolean isDone() {
		
		if (!queue.isEmpty())
			return false;
		
		validateFutures();
		if (!processorsFuture.isEmpty())
			return false;
		
		return true;
	}
	
	protected void progressed() {
		if (_progressListener == null) return;
		_progressListener.progressed(processed.get(), totalItems.get());
	}
	
	private void validateFutures() {
		
		Collection<Future<Object>> remove = new ArrayList<>();
		for (Future<Object> future : processorsFuture) {
			if (future.isDone()) {
				remove.add(future);
				try { future.get(); }
				catch (Throwable exc) {
					errors.add(exc);
				}
			}
		}
		
		for (Future<Object> future : remove)
			processorsFuture.remove(future);
		
		remove.clear();
	}

	public void setMaxThreads(int i) {
		maxThreads = i;	
	}
	
	public void setQueueSizeLimit(int i) {
		queueSizeLimit = i;
	}
}
