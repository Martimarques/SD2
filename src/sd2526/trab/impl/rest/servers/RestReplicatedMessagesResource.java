package sd2526.trab.impl.rest.servers;

import com.google.gson.Gson;
import jakarta.ws.rs.*;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.ContainerResponseFilter;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response.Status;
import jakarta.ws.rs.ext.Provider;
import sd2526.trab.api.Message;
import sd2526.trab.api.java.Result;
import sd2526.trab.api.rest.RestMessages;
import sd2526.trab.impl.api.rest.RestAdminMessages;
import sd2526.trab.impl.kafka.KafkaReplicatedMessages;
import sd2526.trab.impl.kafka.SyncPoint;

import java.util.List;

@Path(RestMessages.PATH)
public class RestReplicatedMessagesResource implements RestMessages, RestAdminMessages {

    private static final String SERVER_SECRET = "SD2526-Password-Secreta";

    static final KafkaReplicatedMessages kafka = new KafkaReplicatedMessages();
    static final Gson gson = new Gson();

    @Context
    HttpHeaders headers;


    private void checkSecret() {
        List<String> secret = headers.getRequestHeader("X-Server-Secret");
        if (secret == null || secret.isEmpty() || !SERVER_SECRET.equals(secret.get(0))) {
            throw new WebApplicationException(Status.FORBIDDEN);
        }
    }

    private KafkaReplicatedMessages.CommandResult waitAndParseResult(long offset) {
        SyncPoint.getSyncPoint().waitForVersion(offset);
        String res = SyncPoint.getSyncPoint().waitForResult(offset);
        return gson.fromJson(res, KafkaReplicatedMessages.CommandResult.class);
    }

    private void throwIfError(KafkaReplicatedMessages.CommandResult cr) {
        if (cr.error == Result.ErrorCode.OK) return;
        Status s = switch (cr.error) {
            case NOT_FOUND   -> Status.NOT_FOUND;
            case CONFLICT    -> Status.CONFLICT;
            case FORBIDDEN   -> Status.FORBIDDEN;
            case BAD_REQUEST -> Status.BAD_REQUEST;
            default          -> Status.INTERNAL_SERVER_ERROR;
        };
        throw new WebApplicationException(s);
    }


    @Override
    @POST
    @Path("/")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public String postMessage(@QueryParam(RestMessages.PWD) String pwd, Message msg) {
        long offset = kafka.publish(KafkaReplicatedMessages.KafkaCommand.OpType.POST, pwd, msg, null, null);
        KafkaReplicatedMessages.CommandResult cr = waitAndParseResult(offset);
        throwIfError(cr);
        return cr.value;
    }

    @Override
    @DELETE
    @Path(RestMessages.MBOX + "/{" + RestMessages.NAME + "}/{" + RestMessages.MID + "}")
    public void removeFromUserInbox(@PathParam(RestMessages.NAME) String name,
                                    @PathParam(RestMessages.MID) String mid,
                                    @QueryParam(RestMessages.PWD) String pwd) {
        long offset = kafka.publish(KafkaReplicatedMessages.KafkaCommand.OpType.REMOVE, pwd, null, name, mid);
        throwIfError(waitAndParseResult(offset));
    }

    @Override
    @DELETE
    @Path("/{" + RestMessages.NAME + "}/{" + RestMessages.MID + "}")
    public void deleteMessage(@PathParam(RestMessages.NAME) String name,
                              @PathParam(RestMessages.MID) String mid,
                              @QueryParam(RestMessages.PWD) String pwd) {
        long offset = kafka.publish(KafkaReplicatedMessages.KafkaCommand.OpType.DELETE, pwd, null, name, mid);
        throwIfError(waitAndParseResult(offset));
    }


    @Override
    @GET
    @Path(RestMessages.MBOX + "/{" + RestMessages.NAME + "}/{" + RestMessages.MID + "}")
    @Produces(MediaType.APPLICATION_JSON)
    public Message getMessage(@PathParam(RestMessages.NAME) String name,
                              @PathParam(RestMessages.MID) String mid,
                              @QueryParam(RestMessages.PWD) String pwd) {
        Result<Message> r = KafkaReplicatedMessages.getDb().getInboxMessage(name, mid, pwd);
        if (r.isOK()) return r.value();
        throwIfError(new KafkaReplicatedMessages.CommandResult(r.error(), null));
        return null;
    }

    @Override
    @GET
    @Path(RestMessages.MBOX + "/{" + RestMessages.NAME + "}")
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> getMessages(@PathParam(RestMessages.NAME) String name,
                                    @QueryParam(RestMessages.PWD) String pwd,
                                    @QueryParam(RestMessages.QUERY) @DefaultValue("") String query) {
        Result<List<String>> r = (query != null && !query.isEmpty())
                ? KafkaReplicatedMessages.getDb().searchInbox(name, pwd, query)
                : KafkaReplicatedMessages.getDb().getAllInboxMessages(name, pwd);
        if (r.isOK()) return r.value();
        throwIfError(new KafkaReplicatedMessages.CommandResult(r.error(), null));
        return null;
    }

    @Override
    @POST
    @Path(RestAdminMessages.ADMIN)
    @Consumes(MediaType.APPLICATION_JSON)
    public void remotePostMessage(Message msg) {
        checkSecret();
        long offset = kafka.publish(KafkaReplicatedMessages.KafkaCommand.OpType.REMOTE_POST, null, msg, null, null);
        waitAndParseResult(offset);
    }

    @Override
    @DELETE
    @Path(RestAdminMessages.ADMIN + "/{" + RestAdminMessages.MID + "}")
    public void remoteDeleteMessage(@PathParam(RestAdminMessages.MID) String mid) {
        checkSecret();
        long offset = kafka.publish(KafkaReplicatedMessages.KafkaCommand.OpType.REMOTE_DELETE, null, null, null, mid);
        waitAndParseResult(offset);
    }

    @Override
    @DELETE
    @Path(RestAdminMessages.ADMIN + "/" + RestAdminMessages.INBOX + "/{" + RestAdminMessages.NAME + "}")
    public void remoteDeleteUserInbox(@PathParam(RestAdminMessages.NAME) String name) {
        checkSecret();
        long offset = kafka.publish(KafkaReplicatedMessages.KafkaCommand.OpType.REMOTE_DELETE_INBOX, null, null, name, null);
        waitAndParseResult(offset);
    }


    @Provider
    public static class CausalFilter implements ContainerRequestFilter, ContainerResponseFilter {

        @Override
        public void filter(ContainerRequestContext req) {
            String cv = req.getHeaderString("X-MESSAGES");
            if (cv != null && !cv.isBlank()) {
                try {
                    long target = Long.parseLong(cv.trim());
                    SyncPoint.getSyncPoint().waitForVersion(target);
                } catch (Exception ignored) {}
            }
        }

        @Override
        public void filter(ContainerRequestContext req, ContainerResponseContext res) {
            if (KafkaReplicatedMessages.currentVersion >= 0)
                res.getHeaders().add("X-MESSAGES", String.valueOf(KafkaReplicatedMessages.currentVersion));
        }
    }
}