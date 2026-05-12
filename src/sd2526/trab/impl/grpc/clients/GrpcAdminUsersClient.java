package sd2526.trab.impl.grpc.clients;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import sd2526.trab.api.java.Result;
import sd2526.trab.impl.api.java.AdminUsers;
import sd2526.trab.impl.grpc.generated_java.AdminUsersProtoBuf.CheckUsersArgs;
import sd2526.trab.impl.grpc.generated_java.GrpcAdminUsersGrpc;
import sd2526.trab.impl.grpc.generated_java.GrpcAdminUsersGrpc.GrpcAdminUsersBlockingStub;

public class GrpcAdminUsersClient extends GrpcClient implements AdminUsers {

	private static final String SERVER_SECRET = "SD2526-Password-Secreta";
	final GrpcAdminUsersBlockingStub admin;

	public GrpcAdminUsersClient(String serverURI) {
		super(serverURI);

		Metadata metadata = new Metadata();
		metadata.put(Metadata.Key.of("x-server-secret", Metadata.ASCII_STRING_MARSHALLER), SERVER_SECRET);

		this.admin = GrpcAdminUsersGrpc.newBlockingStub( super.channel )
				.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
	}

	@Override
	public Result<Set<String>> checkUsers(Collection<String> names) {
		var res = admin.checkUsers( CheckUsersArgs.newBuilder().addAllNames( names).build() );
		return super.toJavaResult( () -> new HashSet<>(res.getUnknownList()));
	}
}