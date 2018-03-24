package worker.discovery;

import com.google.common.collect.Lists;
import com.orbitz.consul.AgentClient;
import com.orbitz.consul.Consul;

/**
 * Created by adambalogh.
 */
public class ConsulClient {
    private final AgentClient agentClient;

    public ConsulClient() {
        Consul consul = Consul.builder().build(); // connect to Consul on localhost
        this.agentClient = consul.agentClient();
    }

    public void register(int port, String serviceName, String serviceId) {
        agentClient.register(port, Lists.newArrayList(), serviceName, serviceId);
    }
}
