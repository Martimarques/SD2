package sd2526.trab.impl.kafka;

import com.google.gson.Gson;
import sd2526.trab.api.Message;
import sd2526.trab.api.java.Result;
import sd2526.trab.impl.java.servers.JavaMessages;
import java.util.List;

public class KafkaReplicatedMessages {
    private static final Gson gson = new Gson();
    public static final String KAFKA_BROKERS = "kafka:9092,localhost:9092";
    public static volatile long currentVersion = -1;

    // Tópico por domínio: cada domínio tem o seu próprio tópico Kafka
    public static final String TOPIC;
    static {
        String temp = "default";
        try {
            String host = java.net.InetAddress.getLocalHost().getHostName();
            if (host.contains(".")) temp = host.substring(host.indexOf('.') + 1);
            else temp = host;
        } catch (Exception e) {}
        TOPIC = "messages_" + temp;
    }

    private static KafkaPublisher publisher;
    private static boolean initialized = false;
    private static final JavaMessages dbImpl = JavaMessages.getInstance();

    public KafkaReplicatedMessages() {
        synchronized (KafkaReplicatedMessages.class) {
            if (!initialized) {
                try {
                    publisher = KafkaPublisher.createPublisher(KAFKA_BROKERS);
                    KafkaSubscriber subscriber = KafkaSubscriber.createSubscriber(KAFKA_BROKERS, List.of(TOPIC));

                    subscriber.start(r -> {
                        try {
                            KafkaCommand cmd = gson.fromJson(r.value(), KafkaCommand.class);
                            Result.ErrorCode err = Result.ErrorCode.INTERNAL_ERROR;
                            String val = null;

                            switch (cmd.type) {
                                case POST:
                                    Result<String> pr = dbImpl.postMessage(cmd.pwd, cmd.msg);
                                    err = pr.error();
                                    // Duplicado de outro réplica — tratar como OK
                                    if (err == Result.ErrorCode.CONFLICT) err = Result.ErrorCode.OK;
                                    if (pr.isOK()) val = pr.value();
                                    break;
                                case REMOVE:
                                    err = dbImpl.removeInboxMessage(cmd.user, cmd.mid, cmd.pwd).error();
                                    break;
                                case DELETE:
                                    err = dbImpl.deleteMessage(cmd.user, cmd.mid, cmd.pwd).error();
                                    if (err == Result.ErrorCode.CONFLICT || err == Result.ErrorCode.NOT_FOUND)
                                        err = Result.ErrorCode.OK;
                                    break;
                                case REMOTE_POST:
                                    err = ((sd2526.trab.impl.api.java.AdminMessages) dbImpl).remotePostMessage(cmd.msg).error();
                                    if (err == Result.ErrorCode.CONFLICT) err = Result.ErrorCode.OK;
                                    break;
                                case REMOTE_DELETE:
                                    err = ((sd2526.trab.impl.api.java.AdminMessages) dbImpl).remoteDeleteMessage(cmd.mid).error();
                                    if (err == Result.ErrorCode.NOT_FOUND) err = Result.ErrorCode.OK;
                                    break;
                                case REMOTE_DELETE_INBOX:
                                    err = ((sd2526.trab.impl.api.java.AdminMessages) dbImpl).remoteDeleteUserInbox(cmd.user).error();
                                    if (err == Result.ErrorCode.NOT_FOUND) err = Result.ErrorCode.OK;
                                    break;
                            }

                            try {
                                SyncPoint.getSyncPoint().setResult(r.offset(),
                                        gson.toJson(new CommandResult(err, val)));
                            } catch (RuntimeException ex) {
                                // offset já processado por outro réplica — ignorar
                            }
                            currentVersion = r.offset();
                        } catch (Exception e) {
                            try {
                                SyncPoint.getSyncPoint().setResult(r.offset(),
                                        gson.toJson(new CommandResult(Result.ErrorCode.INTERNAL_ERROR, null)));
                            } catch (RuntimeException ex) { /* ignorar */ }
                        }
                    });

                    System.out.println("Kafka ligado com sucesso ao tópico: " + TOPIC);
                } catch (Exception e) {
                    System.err.println("Aviso: Falha ao ligar ao Kafka. " + e.getMessage());
                }
                initialized = true;
            }
        }
    }

    public long publish(KafkaCommand.OpType type, String pwd, Message msg, String user, String mid) {
        return publisher.publish(TOPIC, gson.toJson(new KafkaCommand(type, pwd, msg, user, mid)));
    }

    public static JavaMessages getDb() {
        return dbImpl;
    }

    public static class KafkaCommand {
        public enum OpType { POST, REMOVE, DELETE, REMOTE_POST, REMOTE_DELETE, REMOTE_DELETE_INBOX }
        public OpType type; public String pwd; public Message msg; public String user; public String mid;
        public KafkaCommand(OpType t, String p, Message m, String u, String id) {
            type=t; pwd=p; msg=m; user=u; mid=id;
        }
    }

    public static class CommandResult {
        public Result.ErrorCode error; public String value;
        public CommandResult(Result.ErrorCode e, String v) { error=e; value=v; }
    }
}