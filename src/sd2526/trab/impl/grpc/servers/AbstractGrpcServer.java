package sd2526.trab.impl.grpc.servers;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.security.KeyStore;
import java.util.List;
import java.util.logging.Logger;

import javax.net.ssl.KeyManagerFactory;

import io.grpc.Server;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyServerBuilder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;

import sd2526.trab.impl.discovery.Discovery;
import sd2526.trab.impl.java.servers.AbstractServer;


public abstract class AbstractGrpcServer extends AbstractServer {
	private static final String SERVER_BASE_URI = "grpc://%s:%s%s";

	private static final String GRPC_CTX = "/grpc";

	protected final Server server;

	protected AbstractGrpcServer(Logger log, String service, int port) {
		// O URI tem de usar o HostName em vez do IP para o TLS funcionar (como diz no PPT)
		super(log, service, String.format(SERVER_BASE_URI, getHostname(), port, GRPC_CTX));

		try {
			// 1. Ler as propriedades da JVM (Keystore) do PPT
			String keyStoreFilename = System.getProperty("javax.net.ssl.keyStore");
			String keyStorePassword = System.getProperty("javax.net.ssl.keyStorePassword");

			// 2. Carregar a Keystore
			KeyStore keystore = KeyStore.getInstance(KeyStore.getDefaultType());
			try(FileInputStream input = new FileInputStream(keyStoreFilename)) {
				keystore.load(input, keyStorePassword.toCharArray());
			}

			// 3. Inicializar a KeyManagerFactory
			KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(
					KeyManagerFactory.getDefaultAlgorithm());
			keyManagerFactory.init(keystore, keyStorePassword.toCharArray());

			// 4. Criar o Contexto SSL
			SslContext context = GrpcSslContexts.configure(
					SslContextBuilder.forServer(keyManagerFactory)
			).build();

			// 5. Usar NettyServerBuilder com o SslContext injetado
			var builder = NettyServerBuilder.forPort(port).sslContext(context);

			for( var s : controllers( super.serverURI ) ) {
				builder.addService( s );
			}

			this.server = builder.build();

		} catch (Exception e) {
			log.severe("Falhou a inicialização do TLS/gRPC: " + e.getMessage());
			throw new RuntimeException("Erro TLS no gRPC", e);
		}
	}

	// Método auxiliar para ir buscar o Hostname
	private static String getHostname() {
		try {
			return InetAddress.getLocalHost().getHostName();
		} catch (Exception e) {
			return "localhost";
		}
	}

	protected abstract List<GrpcController> controllers( String uri );

	protected void start() throws IOException {

		Discovery.getInstance().announce(serviceName(), super.serverURI);

		Log.info(String.format("%s gRPC Server ready @ %s\n", service, serverURI));

		server.start();
		Runtime.getRuntime().addShutdownHook(new Thread( () -> {
			System.err.println("*** shutting down gRPC server since JVM is shutting down");
			server.shutdownNow();
			System.err.println("*** server shut down");
		}));
	}
}