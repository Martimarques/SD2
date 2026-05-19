package sd2526.trab.impl.rest.servers;

import java.util.logging.Logger;
import org.glassfish.jersey.server.ResourceConfig;
import sd2526.trab.api.java.Messages;
import sd2526.trab.impl.api.zoho.Zoho;

public class RestMessagesProxyServer extends AbstractRestServer {
    public static final int PORT = 8080; // Podes ajustar se o teste pedir outra porta

    private static Logger Log = Logger.getLogger(RestMessagesProxyServer.class.getName());

    RestMessagesProxyServer() {
        super(Log, Messages.SERVICE_NAME, PORT);
    }

    @Override
    void registerResources(ResourceConfig config) {
        // Avisar o recurso que estamos a correr como Proxy do Zoho
        RestMessagesResource.isProxy = true;
        config.register(RestMessagesResource.class);
    }

    public static void main(String[] args) throws Exception {
        // TRUQUE DO ESTADO LIMPO (Requisito do Enunciado)
        // O Tester passa "true" ou "false" no args[0] para indicar se devemos ignorar o lixo.
        if (args.length > 0) {
            boolean drop = Boolean.parseBoolean(args[0]);
            Zoho.getInstance().setDropState(drop);
            System.out.println("ProxyServer: Argumento dropState recebido = " + drop);
        } else {
            // Por omissão, se não passarem argumentos, assumimos "true" para limpar o lixo.
            Zoho.getInstance().setDropState(true);
            System.out.println("ProxyServer: Nenhum argumento recebido, dropState por omissao = true");
        }

        new RestMessagesProxyServer().start();
    }
}