package sd2526.trab.impl.api.zoho;

import java.util.List;
import com.github.scribejava.core.builder.ServiceBuilder;
import com.github.scribejava.core.model.*;
import com.github.scribejava.core.oauth.OAuth20Service;
import com.google.gson.Gson;
import sd2526.trab.api.Message;

public class Zoho {
    static final String MAIL_API_BASE = "https://mail.zoho.eu/api";
    static final String CLIENT_ID = "1000.WETOR47CPUI4UY5WJL1R6SAUYCWKPL";
    static final String CLIENT_SECRET = "9579e96acf2757ea20c84615c5b0abcb6029e7c544";
    static final String REFRESH_TOKEN = "1000.f198b53b1aec8eb37c3ade13ed396330.a9ca2fd863853132bbbc9937b00a5aa8";

    final OAuth20Service service;
    final ZohoTokenManager tokenManager;
    final Gson gson = new Gson();
    static Zoho instance;
    private String accountId = null;
    private String myEmailAddress = null;

    static {
        // Trust all certificates (apenas para desenvolvimento - não usar em produção)
        try {
            javax.net.ssl.TrustManager[] trustAllCerts = new javax.net.ssl.TrustManager[]{
                    new javax.net.ssl.X509TrustManager() {
                        public java.security.cert.X509Certificate[] getAcceptedIssuers() { return null; }
                        public void checkClientTrusted(java.security.cert.X509Certificate[] certs, String authType) {}
                        public void checkServerTrusted(java.security.cert.X509Certificate[] certs, String authType) {}
                    }
            };
            javax.net.ssl.SSLContext sc = javax.net.ssl.SSLContext.getInstance("SSL");
            sc.init(null, trustAllCerts, new java.security.SecureRandom());
            javax.net.ssl.HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
            javax.net.ssl.HttpsURLConnection.setDefaultHostnameVerifier((hostname, session) -> true);
        } catch (Exception e) {}
    }

    private Zoho() {
        service = new ServiceBuilder(CLIENT_ID).apiSecret(CLIENT_SECRET).build(ZohoApi.instance());
        tokenManager = new ZohoTokenManager(service, REFRESH_TOKEN);
    }

    public synchronized static Zoho getInstance() {
        if(instance == null) instance = new Zoho();
        return instance;
    }

    public synchronized void fetchAccountInfo() throws Exception {
        System.out.println("Zoho: fetchAccountInfo() iniciado...");
        OAuth2AccessToken accessToken = new OAuth2AccessToken(tokenManager.getValidAccessToken());
        OAuthRequest request = new OAuthRequest(Verb.GET, MAIL_API_BASE + "/accounts");
        service.signRequest(accessToken, request);
        try (Response response = service.execute(request)) {
            System.out.println("Zoho: fetchAccountInfo response code: " + response.getCode());
            if(response.isSuccessful()) {
                String body = response.getBody();
                System.out.println("Zoho: fetchAccountInfo body: " + body);
                ZohoAccountReply reply = gson.fromJson(body, ZohoAccountReply.class);
                if (reply != null && reply.data != null && !reply.data.isEmpty()) {
                    this.accountId = reply.data.get(0).accountId;
                    this.myEmailAddress = reply.data.get(0).primaryEmailAddress;
                    System.out.println("Zoho: accountId = " + this.accountId);
                    System.out.println("Zoho: myEmailAddress = " + this.myEmailAddress);
                } else {
                    System.err.println("Zoho: fetchAccountInfo - no data in reply");
                }
            } else {
                System.err.println("Zoho: fetchAccountInfo failed: " + response.getCode() + " - " + response.getBody());
            }
        }
    }

    public synchronized String sendEmail(Message msg) throws Exception {
        // Garantir que temos accountId e email
        if (this.accountId == null || this.myEmailAddress == null) {
            System.out.println("Zoho: sendEmail - fetching account info first...");
            fetchAccountInfo();
        }
        if (this.accountId == null || this.myEmailAddress == null) {
            System.err.println("Zoho: sendEmail - accountId or email is null, cannot send.");
            return null;
        }

        OAuth2AccessToken accessToken = new OAuth2AccessToken(tokenManager.getValidAccessToken());

        // Endpoint oficial para envio
        OAuthRequest request = new OAuthRequest(Verb.POST, MAIL_API_BASE + "/accounts/" + accountId + "/messages");

        // Garantir que o conteúdo nunca é nulo
        String bodyText = (msg.getContents() == null || msg.getContents().isEmpty()) ? "Empty message body" : msg.getContents();

        // Para filtrar na leitura, colocamos o destinatário no assunto (ex: ourorg0+0001|to:derek.pippen6@ourorg1)
// Em vez de tentarmos buscar o primeiro elemento com .get(0), juntamos todos os destinatários separados por vírgula
        String recipient = (msg.getDestination() != null && !msg.getDestination().isEmpty()) ? String.join(",", msg.getDestination()) : "unknown";        String subject = msg.getId() + "|to:" + recipient;

        ZohoSendEmailRequest payload = new ZohoSendEmailRequest(
                this.myEmailAddress,
                this.myEmailAddress,  // envia para si mesmo (a conta Zoho)
                subject,
                bodyText);

        String jsonPayload = gson.toJson(payload);
        System.out.println("Zoho: sendEmail payload: " + jsonPayload);

        request.addHeader("Content-Type", "application/json");
        request.setPayload(jsonPayload);
        service.signRequest(accessToken, request);

        try (Response response = service.execute(request)) {
            System.out.println("Zoho: sendEmail response code: " + response.getCode());
            if (response.isSuccessful()) {
                System.out.println("Zoho: Email enviado com sucesso! ID: " + msg.getId());
                return msg.getId();
            } else {
                System.err.println("Zoho Error " + response.getCode() + ": " + response.getBody());
                return null;
            }
        }
    }

    public synchronized List<String> getAllMessages() throws Exception {
        if (this.accountId == null || this.myEmailAddress == null) {
            System.out.println("Zoho: getAllMessages - fetching account info first...");
            fetchAccountInfo();
        }
        if (this.accountId == null) {
            System.err.println("Zoho: getAllMessages - accountId is null, cannot list messages.");
            return List.of();
        }

        for (int i = 0; i < 3; i++) {
            OAuth2AccessToken accessToken = new OAuth2AccessToken(tokenManager.getValidAccessToken());
            OAuthRequest request = new OAuthRequest(Verb.GET, MAIL_API_BASE + "/accounts/" + accountId + "/messages/view");
            service.signRequest(accessToken, request);

            try (Response response = service.execute(request)) {
                System.out.println("Zoho: getAllMessages response code: " + response.getCode());
                if(response.isSuccessful()) {
                    String body = response.getBody();
                    System.out.println("Zoho: getAllMessages body: " + body);
                    ZohoMessageListReply reply = gson.fromJson(body, ZohoMessageListReply.class);
                    if (reply != null && reply.data != null && !reply.data.isEmpty()) {
                        // Devolve todos os IDs (sem filtro aqui – o filtro será feito no proxy)
                        List<String> allIds = reply.data.stream()
                                .map(m -> m.messageId)
                                .toList();
                        System.out.println("Zoho: getAllMessages encontrou " + allIds.size() + " mensagens: " + allIds);
                        return allIds;
                    } else {
                        System.out.println("Zoho: getAllMessages - nenhuma mensagem na caixa de entrada.");
                    }
                } else {
                    System.err.println("Zoho: getAllMessages failed: " + response.getCode() + " - " + response.getBody());
                }
            }
            System.out.println("Zoho: Inbox vazia, a aguardar 2s... (Tentativa " + (i+1) + ")");
            Thread.sleep(2000);
        }
        return List.of();
    }

    // Inner classes para parsing do JSON
    class ZohoAccountReply { List<ZohoAccount> data; }
    class ZohoAccount { String accountId; String primaryEmailAddress; }
    record ZohoSendEmailRequest(String fromAddress, String toAddress, String subject, String content) {}
    class ZohoMessageListReply { List<ZohoMessageData> data; }
    class ZohoMessageData { String messageId; String subject; }
}