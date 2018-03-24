package worker.ring;

import com.google.common.collect.Lists;
import com.orbitz.consul.model.catalog.CatalogService;
import worker.discovery.ServiceWatcher;

import java.util.List;

/**
 * Created by adambalogh.
 */
public class EventListenerAdapter implements ServiceWatcher.Callback {
    private final List<Node> lastNodesAlive = Lists.newArrayList();

    private final Ring.EventListener listener;

    public EventListenerAdapter(Ring.EventListener listener) {
        this.listener = listener;
    }

    public void onServices(List<CatalogService> services) {
        List<Node> currentNodesAlive = Lists.newArrayList();

        for (CatalogService service : services) {
            Node node = new Node(service.getServiceAddress(), service.getServicePort(), service.getServiceId());
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
