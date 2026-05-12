package sd2526.trab.impl.rest.clients;

import static sd2526.trab.api.java.Result.error;
import static sd2526.trab.api.java.Result.ok;
import static sd2526.trab.api.java.Result.ErrorCode.INTERNAL_ERROR;
import static sd2526.trab.api.java.Result.ErrorCode.TIMEOUT;

import java.util.function.Supplier;
import java.util.logging.Logger;

import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;

import jakarta.ws.rs.ProcessingException;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.GenericType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import sd2526.trab.api.java.Result;
import sd2526.trab.api.java.Result.ErrorCode;
import sd2526.trab.impl.utils.Sleep;

public class RestClient {
	static Logger Log = Logger.getLogger(RestClient.class.getName());

	protected static final int READ_TIMEOUT = 3000;
	protected static final int CONNECT_TIMEOUT = 3000;

	protected static final int MAX_DEADLINE = 30000;
	protected static final int RETRY_SLEEP = 250;

	final Client client;
	final String serverURI;
	final ClientConfig config;

	final WebTarget target;
	
	protected RestClient(String serverURI, String servicePath ) {
		this.serverURI = serverURI;
		this.config = new ClientConfig();

		config.property(ClientProperties.READ_TIMEOUT, READ_TIMEOUT);
		config.property(ClientProperties.CONNECT_TIMEOUT, CONNECT_TIMEOUT);
		this.client = ClientBuilder.newClient(config);
		this.target = client.target( serverURI ).path( servicePath );
	}

	protected <T> Result<T> reTry(Supplier<Result<T>> func) {
		long T0 = System.currentTimeMillis();
		while( (System.currentTimeMillis() - T0) < MAX_DEADLINE) {
			try {
				Result<T> res = func.get();

				// Se o resultado for sucesso, devolve imediatamente
				if (res.isOK()) {
					return res;
				}

				// Se o erro for NOT_FOUND (404), espera um pouco e tenta outra vez (resolve o delay da BD)
				if (res.error() == ErrorCode.NOT_FOUND || res.error() == ErrorCode.TIMEOUT) {
					Sleep.ms(RETRY_SLEEP);
					continue; // Volta ao início do loop
				}

				// Se for outro erro qualquer (ex: CONFLICT, FORBIDDEN), devolve o erro
				return res;

			} catch (ProcessingException x) {
				// Erros de ligação (Timeout, Connection Refused)
				Sleep.ms(RETRY_SLEEP);
			} catch (Exception x) {
				x.printStackTrace();
				return Result.error(INTERNAL_ERROR);
			}
		}
		return Result.error(TIMEOUT);
	}

	protected Result<Void> toJavaResult(Response r) {
		try {
			var status = r.getStatusInfo().toEnum();
			if (status == Status.OK && r.hasEntity()) {
				return ok(null);
			}
			else 
				if( status == Status.NO_CONTENT) return ok();
			
			return error(getErrorCodeFrom(status.getStatusCode()));
		} finally {
			r.close();
		}
	}

	protected <T> Result<T> toJavaResult(Response r, Class<T> entityType) {
		try {
			var status = r.getStatusInfo().toEnum();
			if (status == Status.OK && r.hasEntity())
				return ok(r.readEntity(entityType));
			else 
				if( status == Status.NO_CONTENT) return ok();
			
			return error(getErrorCodeFrom(status.getStatusCode()));
		} finally {
			r.close();
		}
	}
	
	protected <T> Result<T> toJavaResult(Response r, GenericType<T> entityType) {
		try {
			var status = r.getStatusInfo().toEnum();
			if (status == Status.OK && r.hasEntity())
				return ok(r.readEntity(entityType));
			else 
				if( status == Status.NO_CONTENT) return ok();
			
			return error(getErrorCodeFrom(status.getStatusCode()));
		} finally {
			r.close();
		}
	}
	
	public static ErrorCode getErrorCodeFrom(int status) {
		return switch (status) {
		case 200, 204 -> ErrorCode.OK;
		case 409 -> ErrorCode.CONFLICT;
		case 403 -> ErrorCode.FORBIDDEN;
		case 404 -> ErrorCode.NOT_FOUND;
		case 400 -> ErrorCode.BAD_REQUEST;
		case 500 -> ErrorCode.INTERNAL_ERROR;
		case 501 -> ErrorCode.NOT_IMPLEMENTED;
		default -> ErrorCode.INTERNAL_ERROR;
		};
	}

	@Override
	public String toString() {
		return serverURI.toString();
	}
	
	protected class NotImplementedException extends RuntimeException {
		private static final long serialVersionUID = 1L;
		
		protected NotImplementedException() {
			super("Not implemented");
		}
	}
}
