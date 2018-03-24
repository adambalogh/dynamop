package worker.ring;

import com.google.common.collect.Lists;
import com.orbitz.consul.model.catalog.CatalogService;
import worker.discovery.ServiceWatcher;

import java.util.List;
import java.util.logging.Logger;

/**
 * Created by adambalogh.
 */
public class EventListenerAdapter implements ServiceWatcher.Callback {
    private static final Logger logger = Logger.getLogger(EventListenerAdapter.class.getName());

    private final List<Node> lastNodesAlive = Lists.newArrayList();

    private final Ring.EventListener listener;

    public EventListenerAdapter(Ring.EventListener listener) {
        this.listener = listener;
    }

    public void onServices(List<CatalogService> services) {
        List<Node> currentNodesAlive = Lists.newArrayList();

        for (CatalogService service : services) {
            Node node = new Node(service.getAddress(), service.getServicePort(), service.getServiceId());
            currentNodesAlive.add(node);
            if (!lastNodesAlive.contains(node)) {
                listener.onNodeJoin(node);
            }
        }

        for (Node node : lastNodesAlive) {
            if (!currentNodesAlive.contains(node)) {
                listener.onNodeLeave(node);
            }
        }

        lastNodesAlive.clear();
        lastNodesAlive.addAll(currentNodesAlive);
    }
}
