package sd2526.trab.impl.api.zoho;

import com.github.scribejava.core.builder.ServiceBuilder;
import com.github.scribejava.core.model.OAuth2AccessToken;
import com.github.scribejava.core.model.OAuthRequest;
import com.github.scribejava.core.model.Response;
import com.github.scribejava.core.model.Verb;
import com.github.scribejava.core.oauth.OAuth20Service;
import com.google.gson.Gson;
import sd2526.trab.api.Message;

import java.util.List;

public class Zoho {
    static final String MAIL_API_BASE = "https://mail.zoho.eu/api";

    // 1. COLA AQUI OS TEUS DADOS
    static final String CLIENT_ID = "O_TEU_CLIENT_ID";
    static final String CLIENT_SECRET = "O_TEU_CLIENT_SECRET";
    static final String REFRESH_TOKEN = "O_TEU_REFRESH_TOKEN_QUE_GERASTE_AGORA";

    private static final String ACCOUNTS = "/accounts";

    final OAuth20Service service;
    final ZohoTokenManager tokenManager;
    static Zoho instance;
    final Gson gson = new Gson();

    private String accountId = null; // Vamos guardar o Account ID aqui
    private String myEmailAddress = null; // Vamos guardar o teu email do Zoho aqui

    private Zoho() {
        service = new ServiceBuilder(CLIENT_ID)
                .apiSecret(CLIENT_SECRET)
                .build(ZohoApi.instance()); // Terás de criar a classe ZohoApi (ver nota abaixo)
        tokenManager = new ZohoTokenManager(service, REFRESH_TOKEN);
    }

    public synchronized static Zoho getInstance() {
        if( instance == null) instance = new Zoho();
        return instance;
    }

    // Método para ir buscar o Account ID (Baseado no Lab 9)
    public void fetchAccountInfo() throws Exception {
        OAuth2AccessToken accessToken = new OAuth2AccessToken(tokenManager.getValidAccessToken());
        OAuthRequest request = new OAuthRequest(Verb.GET, MAIL_API_BASE + ACCOUNTS);
        service.signRequest(accessToken, request);

        try (Response response = service.execute(request)) {
            if(response.isSuccessful()) {
                ZohoAccountReply reply = gson.fromJson(response.getBody(), ZohoAccountReply.class);
                if (reply != null && reply.data != null && !reply.data.isEmpty()) {
                    this.accountId = reply.data.get(0).accountId;
                    this.myEmailAddress = reply.data.get(0).primaryEmailAddress;
                    System.out.println("Zoho Account ID carregado: " + accountId);
                }
            } else {
                throw new RuntimeException("Falha ao buscar conta Zoho: " + response.getBody());
            }
        }
    }

    // Exemplo: Enviar um Email (mapeia para o postMessage)
    public String sendEmail(Message msg) throws Exception {
        if (this.accountId == null) fetchAccountInfo(); // Garante que temos o ID da conta

        OAuth2AccessToken accessToken = new OAuth2AccessToken(tokenManager.getValidAccessToken());
        OAuthRequest request = new OAuthRequest(Verb.POST, MAIL_API_BASE + ACCOUNTS + "/" + accountId + "/messages");

        // Formatar o JSON de envio
        ZohoSendEmailRequest payload = new ZohoSendEmailRequest(
                this.myEmailAddress, // From
                String.join(",", msg.getDestination()), // To
                "Mensagem SD: " + msg.getSender(), // Subject
                msg.getContents() // Content
        );

        request.addHeader("Content-Type", "application/json");
        request.setPayload(gson.toJson(payload));
        service.signRequest(accessToken, request);

        try (Response response = service.execute(request)) {
            if(response.isSuccessful()) {
                // O Zoho responde com os dados do email enviado. Aqui só precisamos de devolver um ID de sucesso.
                return "zoho-" + System.currentTimeMillis();
            } else {
                System.err.println("Erro ao enviar no Zoho: " + response.getBody());
                throw new RuntimeException("Falha no envio");
            }
        }
    }

    // Records/Classes auxiliares para o Gson (JSON)
    class ZohoAccountReply { List<ZohoAccount> data; }
    class ZohoAccount { String accountId; String primaryEmailAddress; }
    record ZohoSendEmailRequest(String fromAddress, String toAddress, String subject, String content) {}
}