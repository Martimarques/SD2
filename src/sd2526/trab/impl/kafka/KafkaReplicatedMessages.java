package sd2526.trab.impl.kafka;

import com.google.gson.Gson;
import sd2526.trab.api.Message;
import sd2526.trab.api.java.Result;
import sd2526.trab.impl.java.servers.JavaMessages;
import sd2526.trab.impl.utils.IP;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class KafkaReplicatedMessages {
    private static final Gson gson = new Gson();
    // Dinâmico: Evita colisão se múltiplos domínios usarem o mesmo Kafka
    public static final String TOPIC = "messages_" + IP.domain();
    public static final String KAFKA_BROKERS = "kafka:9092,localhost:9092";
    public static volatile long currentVersion = -1;

    private static KafkaPublisher publisher;
    private static boolean initialized = false;
    private static final JavaMessages dbImpl = JavaMessages.getInstance();

    public KafkaReplicatedMessages() {
        synchronized (KafkaReplicatedMessages.class) {
            if (!initialized) {
                try {
                    // Garante que o tópico existe antes de subscrever
                    KafkaUtils.createTopic(TOPIC, 1, 1);

                    publisher = KafkaPublisher.createPublisher(KAFKA_BROKERS);
                    KafkaSubscriber subscriber = KafkaSubscriber.createSubscriber(KAFKA_BROKERS, List.of(TOPIC));

                    // Determinar o end-offset inicial do tópico para detetar replay.
                    // Records com offset < initialEndOffset são replay do Kafka
                    // após restart do contentor.
                    long initialEndOffset = 0;
                    try {
                        var tp = new TopicPartition(TOPIC, 0);
                        Map<TopicPartition, Long> endOffsets = subscriber.consumer.endOffsets(Collections.singletonList(tp));
                        initialEndOffset = endOffsets.getOrDefault(tp, 0L);
                    } catch (Exception e) {
                        System.err.println("Aviso: não conseguiu obter end-offset do tópico: " + e.getMessage());
                    }
                    final long replayBoundary = initialEndOffset;
                    if (replayBoundary > 0) {
                        dbImpl.replayMode = true;
                        System.out.println("Kafka replay mode: processando records até offset " + replayBoundary);
                    }

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
                                case REMOTE_POST:
                                    err = dbImpl.remotePostMessage(cmd.msg).error();
                                    break;
                                case REMOTE_DELETE:
                                    err = dbImpl.remoteDeleteMessage(cmd.mid).error();
                                    break;
                                case REMOTE_DELETE_INBOX:
                                    err = dbImpl.remoteDeleteUserInbox(cmd.user).error();
                                    break;
                            }

                            SyncPoint.getSyncPoint().setResult(r.offset(), gson.toJson(new CommandResult(err, val)));
                            currentVersion = r.offset();

                            // Desativar replayMode quando atingimos o fim dos records pré-existentes
                            if (dbImpl.replayMode && r.offset() >= replayBoundary - 1) {
                                dbImpl.replayMode = false;
                                System.out.println("Kafka replay completo no offset " + r.offset());
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            SyncPoint.getSyncPoint().setResult(r.offset(),
                                    gson.toJson(new CommandResult(Result.ErrorCode.INTERNAL_ERROR, null)));
                        }
                    });
                    System.out.println("Kafka ligado com sucesso ao topico: " + TOPIC);
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

    // --- DTOs públicos para o Resource os usar ---
    public static class KafkaCommand {
        public enum OpType {
            POST, REMOVE, DELETE,
            REMOTE_POST, REMOTE_DELETE, REMOTE_DELETE_INBOX
        }
        public OpType type;
        public String pwd;
        public Message msg;
        public String user;
        public String mid;

        public KafkaCommand(OpType t, String p, Message m, String u, String id) {
            type=t; pwd=p; msg=m; user=u; mid=id;
        }
    }

    public static class CommandResult {
        public Result.ErrorCode error;
        public String value;
        public CommandResult(Result.ErrorCode e, String v) { error=e; value=v; }
    }
}