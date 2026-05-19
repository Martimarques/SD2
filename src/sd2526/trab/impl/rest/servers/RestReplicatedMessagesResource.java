package sd2526.trab.impl.rest.servers;

import com.google.gson.Gson;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.ContainerResponseFilter;
import jakarta.ws.rs.core.Response.Status;
import jakarta.ws.rs.ext.Provider;
import sd2526.trab.api.Message;
import sd2526.trab.api.java.Result;
import sd2526.trab.api.rest.RestMessages;
import sd2526.trab.impl.kafka.KafkaReplicatedMessages;
import sd2526.trab.impl.kafka.SyncPoint;
import java.util.List;

public class RestReplicatedMessagesResource implements RestMessages {
    private static final Gson gson = new Gson();
    // Singleton — instanciado uma vez, partilhado por todos os requests
    private static final KafkaReplicatedMessages kafka = new KafkaReplicatedMessages();

    // --- Auxiliares ---
    private String safeWait(long offset) {
        String res = null;
        while (res == null) {
            res = SyncPoint.getSyncPoint().waitForResult(offset);
            if (res == null) { try { Thread.sleep(50); } catch (Exception e) {} }
        }
        return res;
    }

    private Status toStatus(Result.ErrorCode err) {
        return switch (err) {
            case NOT_FOUND  -> Status.NOT_FOUND;
            case CONFLICT   -> Status.CONFLICT;
            case FORBIDDEN  -> Status.FORBIDDEN;
            case BAD_REQUEST -> Status.BAD_REQUEST;
            default         -> Status.INTERNAL_SERVER_ERROR;
        };
    }

    // --- Implementação REST ---
    @Override
    public String postMessage(String pwd, Message msg) {
        long offset = kafka.publish(KafkaReplicatedMessages.KafkaCommand.OpType.POST, pwd, msg, null, null);
        KafkaReplicatedMessages.CommandResult res =
                gson.fromJson(safeWait(offset), KafkaReplicatedMessages.CommandResult.class);
        if (res.error == Result.ErrorCode.OK) return res.value;
        throw new WebApplicationException(toStatus(res.error));
    }

    @Override
    public void removeFromUserInbox(String name, String mid, String pwd) {
        long offset = kafka.publish(KafkaReplicatedMessages.KafkaCommand.OpType.REMOVE, pwd, null, name, mid);
        KafkaReplicatedMessages.CommandResult res =
                gson.fromJson(safeWait(offset), KafkaReplicatedMessages.CommandResult.class);
        if (res.error != Result.ErrorCode.OK) throw new WebApplicationException(toStatus(res.error));
    }

    @Override
    public void deleteMessage(String name, String mid, String pwd) {
        long offset = kafka.publish(KafkaReplicatedMessages.KafkaCommand.OpType.DELETE, pwd, null, name, mid);
        KafkaReplicatedMessages.CommandResult res =
                gson.fromJson(safeWait(offset), KafkaReplicatedMessages.CommandResult.class);
        if (res.error != Result.ErrorCode.OK) throw new WebApplicationException(toStatus(res.error));
    }

    @Override
    public Message getMessage(String name, String mid, String pwd) {
        Result<Message> r = KafkaReplicatedMessages.getDb().getInboxMessage(name, mid, pwd);
        if (r.isOK()) return r.value();
        throw new WebApplicationException(toStatus(r.error()));
    }

    @Override
    public List<String> getMessages(String name, String pwd, String query) {
        Result<List<String>> r = (query != null && !query.isEmpty())
                ? KafkaReplicatedMessages.getDb().searchInbox(name, pwd, query)
                : KafkaReplicatedMessages.getDb().getAllInboxMessages(name, pwd);
        if (r.isOK()) return r.value();
        throw new WebApplicationException(toStatus(r.error()));
    }

    // --- Filtro Causal ---
    @Provider
    public static class CausalFilter implements ContainerRequestFilter, ContainerResponseFilter {
        private static final String HEADER = "X-MESSAGES";

        @Override
        public void filter(ContainerRequestContext req) {
            String cv = req.getHeaderString(HEADER);
            if (cv != null && !cv.isEmpty()) {
                try {
                    long target = Long.parseLong(cv);
                    while (KafkaReplicatedMessages.currentVersion < target) {
                        Thread.sleep(50);
                    }
                } catch (Exception ignored) {}
            }
        }

        @Override
        public void filter(ContainerRequestContext req, ContainerResponseContext res) {
            if (KafkaReplicatedMessages.currentVersion >= 0)
                res.getHeaders().add(HEADER, String.valueOf(KafkaReplicatedMessages.currentVersion));
        }
    }
}