package worker.discovery;

import com.orbitz.consul.CatalogClient;
import com.orbitz.consul.Consul;
import com.orbitz.consul.ConsulException;
import com.orbitz.consul.model.ConsulResponse;
import com.orbitz.consul.model.catalog.CatalogService;

import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Created by adambalogh.
 */
public class ServiceWatcher implements Runnable {
    private static final Logger log = Logger.getLogger(ServiceWatcher.class.getName());

    public interface Callback {
        void onServices(List<CatalogService> services);
    }

    private class ServiceFetchTask extends TimerTask {
        @Override
        public void run() {
            try {
                ConsulResponse<List<CatalogService>> response = catalogClient.getService(serviceToWatch);
                callback.onServices(response.getResponse());
            } catch (ConsulException e) {
                log.warning("Failed to retrieve services from Consul");
            }
        }
    }

    private final CatalogClient catalogClient;
    private final String serviceToWatch;
    private final Callback callback;

    public ServiceWatcher(Consul consul, String serviceToWatch, Callback callback) {
        this.catalogClient = consul.catalogClient();
        this.serviceToWatch = serviceToWatch;
        this.callback = callback;
    }

    public void run() {
        Timer timer = new Timer("ServiceWatcher timer");
        ServiceFetchTask serviceFetchTask = new ServiceFetchTask();
        log.info("Starting ServiceWatcher timer thread");
        timer.scheduleAtFixedRate(serviceFetchTask, 0l, TimeUnit.SECONDS.toMillis(5));
        log.info("ServiceWatcher timer thread stopped");
    }
}
