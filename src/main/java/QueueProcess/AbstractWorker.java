package QueueProcess;

import java.util.ArrayList;
import java.util.Collection;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;

public abstract class AbstractWorker<T> implements Callable<Object> {
	
	AbstractFacilitator<T> _facilitator;
	boolean _isBatchProcessing;
	int _batchSize;
	
	public AbstractWorker(AbstractFacilitator<T> facilitator) {
		this(facilitator, false);
	}
	
	public AbstractWorker(AbstractFacilitator<T> facilitator, boolean isBatchProcessing) {
		this(facilitator, false, 5);
	}
	
	public AbstractWorker(AbstractFacilitator<T> facilitator, boolean isBatchProcessing, int batchSize) {
		_facilitator = facilitator;
		_isBatchProcessing = isBatchProcessing;
		_batchSize = batchSize;
	}

	protected abstract void process(T item) throws Exception;
	
	protected abstract void processBatch(Collection<T> item) throws Exception;

	public Object call() throws Exception {
		
		while (true) {
			
			Collection<T> items = new ArrayList<>();
			
			try {
				if (_isBatchProcessing) {
					while (!_facilitator.queue.isEmpty()
							&& items.size() <= _batchSize) {
						items.add(_facilitator.queue.remove());
					}
				}
				else
					items.add(_facilitator.queue.remove());
			}
			catch (NoSuchElementException exc) {
				if (items.size() == 0)
					return null;
			}

			if (items.size() == 0)
				return null;
			
			try {
				if (_isBatchProcessing) processBatch(items);
				else process(items.iterator().next());
			}
			catch (Exception exc) {
				_facilitator.itemsError(items, exc);
				throw exc;
			}

			_facilitator.itemsDone(items);
		}
	}
	
}
