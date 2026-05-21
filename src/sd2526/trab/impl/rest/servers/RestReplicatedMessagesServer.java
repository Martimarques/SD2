package sd2526.trab.impl.rest.servers;

import java.util.logging.Logger;
import org.glassfish.jersey.server.ResourceConfig;
import sd2526.trab.api.java.Messages;

public class RestReplicatedMessagesServer extends AbstractRestServer {
    public static final int PORT = 4567;
    private static final Logger Log = Logger.getLogger(RestReplicatedMessagesServer.class.getName());

    public RestReplicatedMessagesServer(int port) {
        super(Log, Messages.SERVICE_NAME, port);
    }

    @Override
    protected void registerResources(ResourceConfig config) {
        config.register(RestReplicatedMessagesResource.class);
        config.register(RestReplicatedMessagesResource.CausalFilter.class);
        Log.info("Servidor configurado para usar Replicação com Kafka!");
    }

    public static void main(String[] args) throws Exception {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : PORT;
        new RestReplicatedMessagesServer(port).start();
    }
}