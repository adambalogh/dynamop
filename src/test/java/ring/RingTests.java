package ring;

import org.junit.Before;
import org.junit.Test;
import worker.ring.ConsistentHash;
import worker.ring.Node;
import worker.ring.Ring;

import static com.google.common.truth.Truth.assertThat;
import static junit.framework.TestCase.fail;

/**
 * Created by adambalogh.
 */
public class RingTests {
    private static final Node FIRST_NODE = new Node("url", 1, "a");
    private static final Node SECOND_NODE = new Node("url", 1, "h");

    private Ring ring;

    @Before
    public void before() {
        ring = new Ring();
    }

    @Test
    public void testRing() {
        // If there is one node, everything should be assigned to it
        ring.addNode(FIRST_NODE);
        assertThat(ring.getAssignedNode(0).serviceId).isEqualTo(FIRST_NODE.serviceId);
        assertThat(ring.getAssignedNode(ConsistentHash.NUM_BUCKETS - 1).serviceId).isEqualTo(FIRST_NODE.serviceId);

        ring.addNode(SECOND_NODE);
        assertThat(ring.getAssignedNode(SECOND_NODE.bucket + 1).serviceId).isEqualTo(SECOND_NODE.serviceId);
        assertThat(ring.getAssignedNode(FIRST_NODE.bucket).serviceId).isEqualTo(SECOND_NODE.serviceId);
        assertThat(ring.getAssignedNode(FIRST_NODE.bucket + 1).serviceId).isEqualTo(FIRST_NODE.serviceId);
        assertThat(ring.getAssignedNode(SECOND_NODE.bucket).serviceId).isEqualTo(FIRST_NODE.serviceId);

        ring.removeNode(SECOND_NODE);
        assertThat(ring.getAssignedNode(SECOND_NODE.bucket + 1).serviceId).isEqualTo(FIRST_NODE.serviceId);
    }

    @Test
    public void throwsWhenBucketAlreadyOccupied() {
        ring.addNode(FIRST_NODE);
        try {
            ring.addNode(new Node("url", 1, "a"));
            fail("Expected to throw when bucket is already occupied");
        } catch (Exception e) {
            // expected
        }
    }
}
