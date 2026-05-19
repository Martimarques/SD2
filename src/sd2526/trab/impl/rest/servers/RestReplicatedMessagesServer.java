package sd2526.trab.impl.rest.servers;

import java.util.logging.Logger;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.model.ModelProcessor;
import org.glassfish.jersey.server.model.ResourceModel;
import jakarta.ws.rs.core.Configuration;

import sd2526.trab.api.java.Messages;
import sd2526.trab.impl.kafka.KafkaReplicatedMessagesResource;

public class RestReplicatedMessagesServer extends AbstractRestServer {
    public static final int PORT = 4567;
    private static Logger Log = Logger.getLogger(RestReplicatedMessagesServer.class.getName());

    public RestReplicatedMessagesServer(int port) {
        super(Log, Messages.SERVICE_NAME, port);
    }

    @Override
    protected void registerResources(ResourceConfig config) {
        // O TRUQUE DE MESTRE: Este ModelProcessor apaga a rota original da memória do Jersey
        // Assim, podes manter a classe original no projeto sem dar erro 300!
        config.register(new ModelProcessor() {
            @Override
            public ResourceModel processResourceModel(ResourceModel resourceModel, Configuration configuration) {
                ResourceModel.Builder newModel = new ResourceModel.Builder(false);
                for (org.glassfish.jersey.server.model.Resource res : resourceModel.getResources()) {
                    // Bloqueia a classe original de ser ativada neste servidor
                    if (!res.getName().contains("RestMessagesResource")) {
                        newModel.addResource(res);
                    }
                }
                return newModel.build();
            }
            @Override
            public ResourceModel processSubResource(ResourceModel resourceModel, Configuration configuration) {
                return resourceModel;
            }
        });

        // Regista a nossa classe 2-em-1 e o filtro
        config.register(KafkaReplicatedMessagesResource.class);
        config.register(KafkaReplicatedMessagesResource.CausalFilter.class);

        Log.info("Servidor configurado para usar Replicação com Kafka!");
    }

    public static void main(String[] args) throws Exception {
        // Lê a porta dos argumentos do Tester, ou usa a predefinida
        int port = args.length > 0 ? Integer.parseInt(args[0]) : PORT;
        new RestReplicatedMessagesServer(port).start();
    }
}