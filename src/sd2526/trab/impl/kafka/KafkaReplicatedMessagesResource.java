package sd2526.trab.impl.kafka;

import java.util.List;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response.Status;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.ContainerResponseFilter;
import jakarta.ws.rs.ext.Provider;
import com.google.gson.Gson;

import sd2526.trab.api.Message;
import sd2526.trab.api.java.Result;
import sd2526.trab.api.rest.RestMessages;
import sd2526.trab.impl.java.servers.JavaMessages;

public class KafkaReplicatedMessagesResource implements RestMessages {
    private static final Gson gson = new Gson();
    private static KafkaPublisher publisher;
    private static final String topic = "messages_topic";
    private static final JavaMessages dbImpl = JavaMessages.getInstance();

    // Volatile garante que as threads do Jersey vêm logo as atualizações do Kafka
    public static volatile long currentVersion = -1;

    private static boolean initialized = false;

    public KafkaReplicatedMessagesResource() {
        synchronized(KafkaReplicatedMessagesResource.class) {
            if (!initialized) {
                try {
                    String KAFKA_BROKERS = "kafka:9092,localhost:9092";
                    publisher = KafkaPublisher.createPublisher(KAFKA_BROKERS);
                    KafkaSubscriber subscriber = KafkaSubscriber.createSubscriber(KAFKA_BROKERS, List.of(topic));

                    subscriber.start(r -> {
                        try {
                            KafkaCommand cmd = gson.fromJson(r.value(), KafkaCommand.class);
                            Result.ErrorCode err = Result.ErrorCode.INTERNAL_ERROR;
                            String val = null;

                            switch (cmd.type) {
                                case POST:
                                    Result<String> pr = dbImpl.postMessage(cmd.pwd, cmd.msg);
                                    err = pr.error();
                                    if (pr.isOK()) val = pr.value();
                                    break;
                                case REMOVE:
                                    err = dbImpl.removeInboxMessage(cmd.user, cmd.mid, cmd.pwd).error();
                                    break;
                                case DELETE:
                                    err = dbImpl.deleteMessage(cmd.user, cmd.mid, cmd.pwd).error();
                                    break;
                            }

                            SyncPoint.getSyncPoint().setResult(r.offset(), gson.toJson(new CommandResult(err, val)));
                            // Atualiza a versão global mal a Base de Dados acabe de gravar!
                            currentVersion = r.offset();
                        } catch (Exception e) {
                            SyncPoint.getSyncPoint().setResult(r.offset(), gson.toJson(new CommandResult(Result.ErrorCode.INTERNAL_ERROR, null)));
                        }
                    });
                    System.out.println("Kafka ligado com sucesso ao topico: " + topic);
                } catch (Exception e) {
                    System.err.println("Aviso: Falha ao ligar ao Kafka. " + e.getMessage());
                }
                initialized = true;
            }
        }
    }

    // --- DTOs Internos ---
    private static class KafkaCommand {
        enum OpType { POST, REMOVE, DELETE }
        OpType type; String pwd; Message msg; String user; String mid;
        KafkaCommand(OpType t, String p, Message m, String u, String id) { type=t; pwd=p; msg=m; user=u; mid=id; }
    }

    private static class CommandResult {
        Result.ErrorCode error; String value;
        CommandResult(Result.ErrorCode e, String v) { error=e; value=v; }
    }

    // --- Auxiliares ---
    private String safeWait(long offset) {
        String res = null;
        while (res == null) {
            res = SyncPoint.getSyncPoint().waitForResult(offset);
            if (res == null) { try { Thread.sleep(50); } catch (Exception e) {} }
        }
        return res;
    }

    private Status status(Result.ErrorCode err) {
        return switch (err) {
            case NOT_FOUND -> Status.NOT_FOUND;
            case CONFLICT -> Status.CONFLICT;
            case FORBIDDEN -> Status.FORBIDDEN;
            case BAD_REQUEST -> Status.BAD_REQUEST;
            default -> Status.INTERNAL_SERVER_ERROR;
        };
    }

    // --- Implementação REST ---
    @Override
    public String postMessage(String pwd, Message msg) {
        long offset = publisher.publish(topic, gson.toJson(new KafkaCommand(KafkaCommand.OpType.POST, pwd, msg, null, null)));
        CommandResult res = gson.fromJson(safeWait(offset), CommandResult.class);
        if (res.error == Result.ErrorCode.OK) return res.value;
        throw new WebApplicationException(status(res.error));
    }

    @Override
    public void removeFromUserInbox(String name, String mid, String pwd) {
        long offset = publisher.publish(topic, gson.toJson(new KafkaCommand(KafkaCommand.OpType.REMOVE, pwd, null, name, mid)));
        CommandResult res = gson.fromJson(safeWait(offset), CommandResult.class);
        if (res.error != Result.ErrorCode.OK) throw new WebApplicationException(status(res.error));
    }

    @Override
    public void deleteMessage(String name, String mid, String pwd) {
        long offset = publisher.publish(topic, gson.toJson(new KafkaCommand(KafkaCommand.OpType.DELETE, pwd, null, name, mid)));
        CommandResult res = gson.fromJson(safeWait(offset), CommandResult.class);
        if (res.error != Result.ErrorCode.OK) throw new WebApplicationException(status(res.error));
    }

    @Override
    public Message getMessage(String name, String mid, String pwd) {
        Result<Message> r = dbImpl.getInboxMessage(name, mid, pwd);
        if (r.isOK()) return r.value();
        throw new WebApplicationException(status(r.error()));
    }

    @Override
    public List<String> getMessages(String name, String pwd, String query) {
        Result<List<String>> r = (query != null && !query.isEmpty()) ? dbImpl.searchInbox(name, pwd, query) : dbImpl.getAllInboxMessages(name, pwd);
        if (r.isOK()) return r.value();
        throw new WebApplicationException(status(r.error()));
    }

    // --- Filtro Causal (CORRIGIDO PARA CONTORNAR O BUG DO SYNCPOINT) ---
    @Provider
    public static class CausalFilter implements ContainerRequestFilter, ContainerResponseFilter {
        private static final String HEADER_NAME = "X-MESSAGES";

        @Override
        public void filter(ContainerRequestContext req) {
            String cv = req.getHeaderString(HEADER_NAME);
            if (cv != null && !cv.isEmpty()) {
                try {
                    long targetVersion = Long.parseLong(cv);
                    // Fazemos o nosso próprio "wait" seguro.
                    // Se o servidor for mais lento que o pedido, ele fica aqui "preso" a piscar até a mensagem entrar na BD.
                    while (currentVersion < targetVersion) {
                        Thread.sleep(50);
                    }
                } catch (Exception e) {
                    // Ignora números inválidos
                }
            }
        }

        @Override
        public void filter(ContainerRequestContext req, ContainerResponseContext res) {
            // Devolve sempre ao cliente o quão atualizada a base de dados está
            if (currentVersion >= 0) {
                res.getHeaders().add(HEADER_NAME, String.valueOf(currentVersion));
            }
        }
    }
}