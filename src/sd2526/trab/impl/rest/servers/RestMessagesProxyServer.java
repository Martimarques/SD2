package sd2526.trab.impl.rest.servers;

import java.util.logging.Logger;
import org.glassfish.jersey.server.ResourceConfig;
import sd2526.trab.api.java.Messages;

public class RestMessagesProxyServer extends AbstractRestServer {
    public static final int PORT = 8080; // Podes ajustar se o teste pedir outra porta

    private static Logger Log = Logger.getLogger(RestMessagesProxyServer.class.getName());

    RestMessagesProxyServer() {
        super(Log, Messages.SERVICE_NAME, PORT);
    }

    @Override
    void registerResources(ResourceConfig config) {
        // Avisar o recurso que estamos a correr como Proxy do Zoho
        RestMessagesResource.isProxy = true;
        config.register(RestMessagesResource.class);
    }

    public static void main(String[] args) {
        new RestMessagesProxyServer().start();
    }
}