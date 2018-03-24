package worker.ring;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

/**
 * Created by adambalogh.
 */
@ThreadSafe
public class Ring {
    private static final Logger log = Logger.getLogger(Ring.class.getName());

    @ThreadSafe
    public class EventListener {
        public void onNodeJoin(Node node) {
            Ring.this.addNode(node);
        }

        public void onNodeLeave(Node node) {
            Ring.this.removeNode(node);
        }
    }

    private final List<Node> nodes = Lists.newArrayList();
    private final Set<Integer> takenBuckets = Sets.newHashSet();

    public synchronized void addNode(Node node) {
        log.info("New node joining the ring: " + node.serviceId);
        Preconditions.checkArgument(!takenBuckets.contains(node.bucket), "Bucket already taken by other node");

        nodes.add(node);
        takenBuckets.add(node.bucket);
        Collections.sort(nodes);
    }

    public synchronized void removeNode(Node node) {
        for (int i = 0; i < nodes.size(); i++) {
            if (nodes.get(i).serviceId == node.serviceId) {
                log.info("Node leaving the ring: " + node.serviceId);
                nodes.remove(i);
                takenBuckets.remove(node.bucket);
                break;
            }
        }
    }

    public synchronized Node getAssignedNode(int bucket) {
        for (int i = nodes.size() - 1; i >= 0; i--) {
            if (nodes.get(i).bucket < bucket) {
                return nodes.get(i);
            }
        }
        return nodes.get(nodes.size() - 1);
    }

    public EventListener newEventListener() {
        return new EventListener();
    }
}
