package sd2526.trab.impl.rest.servers;

import com.google.gson.Gson;
import jakarta.ws.rs.*;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.ContainerResponseFilter;
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

    static final KafkaReplicatedMessages kafka = new KafkaReplicatedMessages();
    static final Gson gson = new Gson();

    // -----------------------------------------------------------------------
    // Utilitário: espera pelo resultado do Kafka e lança exceção se erro
    // -----------------------------------------------------------------------
    private KafkaReplicatedMessages.CommandResult waitAndParseResult(long offset) {
        String res = SyncPoint.getSyncPoint().waitForResult(offset);
        while (res == null) {
            try { Thread.sleep(50); } catch (Exception ignored) {}
            res = SyncPoint.getSyncPoint().waitForResult(offset);
        }
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

    // -----------------------------------------------------------------------
    // RestMessages — operações de escrita passam pelo Kafka
    // -----------------------------------------------------------------------

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

    // -----------------------------------------------------------------------
    // RestMessages — leituras diretas da BD (não passam pelo Kafka)
    // -----------------------------------------------------------------------

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

    // -----------------------------------------------------------------------
    // RestAdminMessages — comunicação entre domínios, passa pelo Kafka
    // -----------------------------------------------------------------------

    @Override
    @POST
    @Path(RestAdminMessages.ADMIN)
    @Consumes(MediaType.APPLICATION_JSON)
    public void remotePostMessage(Message msg) {
        long offset = kafka.publish(KafkaReplicatedMessages.KafkaCommand.OpType.REMOTE_POST, null, msg, null, null);
        waitAndParseResult(offset); // sem verificar erro (duplicados são OK)
    }

    @Override
    @DELETE
    @Path(RestAdminMessages.ADMIN + "/{" + RestAdminMessages.MID + "}")
    public void remoteDeleteMessage(@PathParam(RestAdminMessages.MID) String mid) {
        long offset = kafka.publish(KafkaReplicatedMessages.KafkaCommand.OpType.REMOTE_DELETE, null, null, null, mid);
        waitAndParseResult(offset);
    }

    @Override
    @DELETE
    @Path(RestAdminMessages.ADMIN + "/" + RestAdminMessages.INBOX + "/{" + RestAdminMessages.NAME + "}")
    public void remoteDeleteUserInbox(@PathParam(RestAdminMessages.NAME) String name) {
        long offset = kafka.publish(KafkaReplicatedMessages.KafkaCommand.OpType.REMOTE_DELETE_INBOX, null, null, name, null);
        waitAndParseResult(offset);
    }

    // -----------------------------------------------------------------------
    // Filtro de Causalidade (X-MESSAGES header)
    // -----------------------------------------------------------------------

    @Provider
    public static class CausalFilter implements ContainerRequestFilter, ContainerResponseFilter {

        @Override
        public void filter(ContainerRequestContext req) {
            String cv = req.getHeaderString("X-MESSAGES");
            if (cv != null && !cv.isBlank()) {
                try {
                    long target = Long.parseLong(cv.trim());
                    while (KafkaReplicatedMessages.currentVersion < target) {
                        Thread.sleep(50);
                    }
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