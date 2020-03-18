package QueueProcess;

public interface IProgressListener {

	default void progressed(int progress, int total) {
		progressed(progress, total, null);
	}
	
	void progressed(int progress, int total, String statusText);
	
	default void status(String statusText) { }
}
