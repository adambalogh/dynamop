package worker.ring;

/**
 * Created by adambalogh.
 */
public class ConsistentHash {
    public static final int NUM_BUCKETS = 1_000_003; // should be a prime

    public static int getBucket(String key) {
        return key.hashCode() % NUM_BUCKETS;
    }
}
