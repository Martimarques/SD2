package sd2526.trab.impl.grpc.clients;

import static sd2526.trab.impl.grpc.common.DataModelAdaptor.Message_to_GrpcAdminMessage;

import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import sd2526.trab.api.Message;
import sd2526.trab.api.java.Result;
import sd2526.trab.impl.api.java.AdminMessages;
import sd2526.trab.impl.grpc.generated_java.AdminMessagesProtoBuf.RemoteDeleteMessageArgs;
import sd2526.trab.impl.grpc.generated_java.AdminMessagesProtoBuf.RemoteDeleteUserInboxArgs;
import sd2526.trab.impl.grpc.generated_java.GrpcAdminMessagesGrpc;
import sd2526.trab.impl.grpc.generated_java.GrpcAdminMessagesGrpc.GrpcAdminMessagesBlockingStub;

public class GrpcAdminMessagesClient extends GrpcClient implements AdminMessages {

	private static final String SERVER_SECRET = "SD2526-Password-Secreta";
	final GrpcAdminMessagesBlockingStub admin;

	public GrpcAdminMessagesClient(String serverUrl) {
		super(serverUrl);

		Metadata metadata = new Metadata();
		metadata.put(Metadata.Key.of("x-server-secret", Metadata.ASCII_STRING_MARSHALLER), SERVER_SECRET);

		this.admin = GrpcAdminMessagesGrpc.newBlockingStub( super.channel )
				.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
	}

	@Override
	public Result<Void> remotePostMessage(Message msg) {
		return super.toJavaResult( () -> admin.remotePostMessage( Message_to_GrpcAdminMessage(msg) ) ).mapToVoid();
	}

	@Override
	public Result<Void> remoteDeleteMessage(String mid) {
		return super.toJavaResult( () -> admin.remoteDeleteMessage( RemoteDeleteMessageArgs.newBuilder().setMid(mid).build() ) ).mapToVoid();
	}

	@Override
	public Result<Void> remoteDeleteUserInbox(String name) {
		return super.toJavaResult( () -> admin.remoteDeleteUserInbox( RemoteDeleteUserInboxArgs.newBuilder().setName(name).build() ) ).mapToVoid();
	}
}