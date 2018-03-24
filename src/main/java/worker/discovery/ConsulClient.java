package worker.discovery;

import com.google.common.collect.Lists;
import com.orbitz.consul.AgentClient;
import com.orbitz.consul.Consul;
import com.orbitz.consul.ConsulException;
import com.orbitz.consul.model.agent.Registration;

import java.util.logging.Logger;

/**
 * Created by adambalogh.
 */
public class ConsulClient {
    private static final Logger log = Logger.getLogger(ServiceWatcher.class.getName());

    private final AgentClient agentClient;

    public ConsulClient(Consul consul) {
        this.agentClient = consul.agentClient();
    }

    public void register(int port, String serviceName, String serviceId) throws ConsulException {
        try {
            Registration.RegCheck grpcCheck = Registration.RegCheck.grpc(serviceId, 10);
            agentClient.register(port, Lists.newArrayList(grpcCheck), serviceName, serviceId);
        } catch (ConsulException e) {
            log.warning("Failed to register service: " + e);
        }
    }
}
