package QueueProcess;

import java.util.Collection;

public interface ICallback {

	void done(Collection<Throwable> errors);
}
