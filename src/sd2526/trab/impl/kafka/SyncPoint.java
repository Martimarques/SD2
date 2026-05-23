package sd2526.trab.impl.kafka;

import java.util.concurrent.ConcurrentHashMap;

public class SyncPoint {

	private final ConcurrentHashMap<Long, String> result;
	private long version;

	private SyncPoint() {
		this.result = new ConcurrentHashMap<Long, String>();
		this.version = -1;
	}

	private static SyncPoint instance = null;

	public static synchronized SyncPoint getSyncPoint() {
		if (SyncPoint.instance == null)
			SyncPoint.instance = new SyncPoint();
		return SyncPoint.instance;
	}

	public synchronized String waitForResult(long n) {
		while (version < n) {
			try {
				wait();
			} catch (InterruptedException e) {
				// nothing to be done here
			}
		}
		return result.remove(n);
	}

	public synchronized void waitForVersion(long n) {
		while (version < n) {
			try {
				wait();
			} catch (InterruptedException e) {
				// nothing to be done here
			}
		}
	}

	public synchronized void setResult(long n, String res) {
		// If this offset was already processed (e.g. by another replica's Kafka consumer),
		// just update version if needed and return — do NOT throw.
		if (n < version) {
			return;
		}
		if (res != null) {
			// Only store if not already present (first replica wins)
			result.putIfAbsent(n, res);
		}
		version = n;
		notifyAll();
	}

	public synchronized long getVersion() {
		return this.version;
	}
}