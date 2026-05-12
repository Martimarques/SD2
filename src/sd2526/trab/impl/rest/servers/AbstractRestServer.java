package sd2526.trab.impl.rest.servers;

import java.net.InetAddress;
import java.net.URI;
import java.util.logging.Logger;
import javax.net.ssl.SSLContext; // Importação necessária para o TLS

import org.glassfish.jersey.jdkhttp.JdkHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import sd2526.trab.impl.discovery.Discovery;
import sd2526.trab.impl.java.servers.AbstractServer;
import sd2526.trab.impl.utils.IP;

public abstract class AbstractRestServer extends AbstractServer {
	private static final String SERVER_BASE_URI = "https://%s:%s%s";
	private static final String REST_CTX = "/rest";

	static {
		System.setProperty("java.net.preferIPv4Stack", "true");
		System.setProperty("java.util.logging.SimpleFormatter.format", "%4$s: %5$s\n");
	}

	private final int port;

	protected AbstractRestServer(Logger log, String service, int port) {
		super(log, service, String.format(SERVER_BASE_URI, getHostnameLocal(), port, REST_CTX));
		this.port = port;
	}

	private static String getHostnameLocal() {
		try {
			return InetAddress.getLocalHost().getHostName();
		} catch (Exception e) {
			e.printStackTrace();
			return "localhost";
		}
	}

	protected void start() {
		try {
			ResourceConfig config = new ResourceConfig();

			registerResources( config );

			URI bindUri = URI.create(String.format(SERVER_BASE_URI, INETADDR_ANY, port, REST_CTX));

			JdkHttpServerFactory.createHttpServer(bindUri, config, SSLContext.getDefault());

			if( service != null )
				Discovery.getInstance().announce(serviceName(), super.serverURI);

			Log.info(String.format("%s Server ready @ %s\n",  service, serverURI));

		} catch (Exception e) {
			Log.severe("Failed to start server: " + e.getMessage());
		}
	}

	abstract void registerResources( ResourceConfig config );
}