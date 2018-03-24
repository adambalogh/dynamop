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
public class ServiceWatcher {
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

    private final Timer timer;

    private final CatalogClient catalogClient;
    private final String serviceToWatch;
    private final Callback callback;

    public ServiceWatcher(Consul consul, String serviceToWatch, Callback callback) {
        this.catalogClient = consul.catalogClient();
        this.serviceToWatch = serviceToWatch;
        this.callback = callback;
        this.timer = new Timer("ServiceWatcher Timer");
    }

    /*
     * Starts watching the given in a background thread. This method returns immediately.
     */
    public void start() {
        log.info("Starting ServiceWatcher timer thread");
        timer.scheduleAtFixedRate(new ServiceFetchTask(), 0l, TimeUnit.SECONDS.toMillis(5));
        log.info("ServiceWatcher timer thread stopped");
    }

    public void stop() {
        log.info("Stopping ServiceWatcher");
        timer.cancel();
    }
}
