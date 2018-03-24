package worker.ring;

import com.google.common.base.MoreObjects;

/**
 * Created by adambalogh.
 */
public class Node implements Comparable<Node> {
    public final String url;
    public final int port;
    public final String serviceId;
    public final int bucket;

    public Node(String url, int port, String serviceId) {
        this.url = url;
        this.port = port;
        this.serviceId = serviceId;
        this.bucket = ConsistentHash.getBucket(serviceId);
    }

    public int compareTo(Node other) {
        if (this.bucket <= other.bucket) {
            return -1;
        }
        return 1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null) {
            return false;
        }
        if (getClass() != o.getClass()) {
            return false;
        }

        Node other = (Node) o;
        return this.serviceId.equals(other.serviceId);
    }

    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("url", url)
                .add("port", port)
                .add("serviceId", serviceId)
                .add("bucket", bucket)
                .toString();
    }
}
