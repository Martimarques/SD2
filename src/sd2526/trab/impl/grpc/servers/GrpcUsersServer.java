package sd2526.trab.impl.grpc.servers;

import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import sd2526.trab.api.java.Users;

public class GrpcUsersServer extends AbstractGrpcServer {
	public static final int PORT = 13456;

	private static Logger Log = Logger.getLogger(GrpcUsersServer.class.getName());

	public GrpcUsersServer() {
		super( Log, Users.SERVICE_NAME, PORT);
	}

	@Override
	protected List<ServerServiceDefinition> controllers(String uri) {

		// Interceptor inline (classe anónima) para não criar ficheiro novo
		ServerInterceptor secretAuth = new ServerInterceptor() {
			private static final String SERVER_SECRET = "SD2526-Password-Secreta";
			private static final Metadata.Key<String> SECRET_KEY = Metadata.Key.of("x-server-secret", Metadata.ASCII_STRING_MARSHALLER);

			@Override
			public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
					ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {

				String secret = headers.get(SECRET_KEY);
				if (SERVER_SECRET.equals(secret)) {
					return next.startCall(call, headers);
				} else {
					call.close(Status.PERMISSION_DENIED.withDescription("Autenticacao entre servidores falhou."), headers);
					return new ServerCall.Listener<ReqT>() {};
				}
			}
		};

		return List.of(
				new GrpcUsersController().bindService(),
				ServerInterceptors.intercept(new GrpcAdminUsersController(), secretAuth)
		);
	}

	public static void main(String[] args) {
		try {
			new GrpcUsersServer().start();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}