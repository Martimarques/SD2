package sd2526.trab.impl.rest.servers;

import java.util.logging.Logger;
import org.glassfish.jersey.server.ResourceConfig;

public class RestGatewayServer extends AbstractRestServer {

	public static final int PORT = 6666;

	private static Logger Log = Logger.getLogger(RestGatewayServer.class.getName());

	RestGatewayServer() {
		super(Log, null, PORT);
	}

	@Override
	void registerResources(ResourceConfig config) {
		// 1. Avisar os recursos que estamos a correr em modo Gateway
		RestUsersResource.isGateway = true;
		RestMessagesResource.isGateway = true;

		// 2. Registar as classes em vez das instâncias para o Jersey detetar os @Path corretamente
		config.register(RestUsersResource.class);
		config.register(RestMessagesResource.class);
	}

	public static void main(String[] args) {
		new RestGatewayServer().start();
	}
}