package sd2526.trab.impl.api.zoho;

import java.util.Base64;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
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

    // CORREÇÃO 1: Usar estruturas Thread-Safe para não precisarmos de trancar tudo!
    private final java.util.Set<String> ignoredZohoIds = ConcurrentHashMap.newKeySet();
    private final java.util.Set<String> deletedMids = ConcurrentHashMap.newKeySet();
    private volatile boolean isCleanStateInitialized = false;
    private volatile boolean firstReadDone = false; // Controla o tempo de espera do recomeço

    private boolean dropState = true;

    static {
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

    public synchronized void setDropState(boolean dropState) {
        this.dropState = dropState;
    }

    public synchronized void fetchAccountInfo() throws Exception {
        OAuth2AccessToken accessToken = new OAuth2AccessToken(tokenManager.getValidAccessToken());
        OAuthRequest request = new OAuthRequest(Verb.GET, MAIL_API_BASE + "/accounts");
        service.signRequest(accessToken, request);

        try (Response response = service.execute(request)) {
            if(response.isSuccessful()) {
                ZohoAccountReply reply = gson.fromJson(response.getBody(), ZohoAccountReply.class);
                if (reply != null && reply.data != null && !reply.data.isEmpty()) {
                    this.accountId = reply.data.get(0).accountId;
                    this.myEmailAddress = reply.data.get(0).incomingUserName;
                }
            }
        }
    }

    private synchronized void initCleanState() throws Exception {
        if (isCleanStateInitialized) return;
        isCleanStateInitialized = true; // Marca logo como feito

        if (!dropState) {
            System.out.println("Zoho: Restart com DROP_STATE=false. A manter as mensagens anteriores!");
            // Removemos o sleep daqui para não trancar a porta!
            return;
        }

        System.out.println("Zoho: A inicializar estado limpo (ignorando mensagens antigas)...");
        if (this.accountId == null) fetchAccountInfo();
        if (this.accountId != null) {
            OAuth2AccessToken accessToken = new OAuth2AccessToken(tokenManager.getValidAccessToken());
            OAuthRequest request = new OAuthRequest(Verb.GET, MAIL_API_BASE + "/accounts/" + accountId + "/messages/view");
            service.signRequest(accessToken, request);
            try (Response response = service.execute(request)) {
                if (response.isSuccessful()) {
                    ZohoMessageListReply reply = gson.fromJson(response.getBody(), ZohoMessageListReply.class);
                    if (reply != null && reply.data != null) {
                        for (ZohoMessageData m : reply.data) {
                            ignoredZohoIds.add(m.messageId);
                        }
                    }
                    System.out.println("Zoho: Ignoradas " + ignoredZohoIds.size() + " mensagens que ja la estavam (Lixo).");
                }
            }
        }
    }

    // CORREÇÃO 2: Removido o synchronized daqui!
    public void deleteMessage(String mid) {
        deletedMids.add(mid);
        System.out.println("Zoho: Mensagem " + mid + " apagada virtualmente.");
    }

    // CORREÇÃO 3: Removido o synchronized daqui para permitir a entrada de retransmissões em tempo real!
    public String sendEmail(Message msg) throws Exception {
        if (!isCleanStateInitialized) initCleanState();

        if (this.accountId == null || this.myEmailAddress == null) fetchAccountInfo();
        if (this.accountId == null || this.myEmailAddress == null) return null;

        OAuth2AccessToken accessToken = new OAuth2AccessToken(tokenManager.getValidAccessToken());
        OAuthRequest request = new OAuthRequest(Verb.POST, MAIL_API_BASE + "/accounts/" + accountId + "/messages");

        String jsonMsg = gson.toJson(msg);
        String base64Json = Base64.getEncoder().encodeToString(jsonMsg.getBytes());

        String originalContent = (msg.getContents() == null) ? "" : msg.getContents();
        String bodyText = originalContent + "<br>------<br>" + base64Json;

        String recipient = (msg.getDestination() != null && !msg.getDestination().isEmpty()) ?
                msg.getDestination().iterator().next() : "unknown";

        String subject = msg.getId() + "|to:" + recipient;

        ZohoSendEmailRequest payload = new ZohoSendEmailRequest(
                this.myEmailAddress, this.myEmailAddress, subject, bodyText);

        request.addHeader("Content-Type", "application/json");
        request.setPayload(gson.toJson(payload));
        service.signRequest(accessToken, request);

        try (Response response = service.execute(request)) {
            return response.isSuccessful() ? msg.getId() : null;
        }
    }

    // CORREÇÃO 4: Removido o synchronized!
    public List<String> getAllMessages(String expectedRecipient) throws Exception {
        if (!isCleanStateInitialized) initCleanState();
        if (this.accountId == null || this.myEmailAddress == null) fetchAccountInfo();
        if (this.accountId == null) return List.of();

        // Se acabámos de recomeçar (dropState=false), o Tester pode estar à espera
        // de retransmissões do outro servidor (que demoram algum tempo a chegar).
        // Aumentamos o número de tentativas (polling) temporariamente de 3 para 8.
        int maxTentativas = 3;
        if (!dropState && !firstReadDone) {
            System.out.println("Zoho: Primeira leitura apos crash. A ativar Polling Ativo longo...");
            maxTentativas = 8; // 8 tentativas * 2 segundos = 16s de paciência máxima
            firstReadDone = true;
        }

        List<String> lastKnownList = List.of();

        for (int i = 0; i < maxTentativas; i++) {
            OAuth2AccessToken accessToken = new OAuth2AccessToken(tokenManager.getValidAccessToken());
            OAuthRequest request = new OAuthRequest(Verb.GET, MAIL_API_BASE + "/accounts/" + accountId + "/messages/view");
            service.signRequest(accessToken, request);

            try (Response response = service.execute(request)) {
                if(response.isSuccessful()) {
                    ZohoMessageListReply reply = gson.fromJson(response.getBody(), ZohoMessageListReply.class);
                    if (reply != null && reply.data != null && !reply.data.isEmpty()) {

                        lastKnownList = reply.data.stream()
                                .filter(m -> !ignoredZohoIds.contains(m.messageId))
                                .filter(m -> m.subject != null && m.subject.contains("|to:" + expectedRecipient))
                                .map(m -> m.subject.split("\\|to:")[0])
                                .filter(mid -> !deletedMids.contains(mid))
                                .distinct()
                                .toList();

                        // Se for uma leitura normal (maxTentativas=3), devolve logo o que encontrar.
                        // Se estivermos em recuperação de falha (maxTentativas=8),
                        // só devolve logo se tiver encontrado mensagens "novas" ou múltiplas mensagens.
                        if (maxTentativas == 3 || lastKnownList.size() > 1) {
                            return lastKnownList;
                        }
                    }
                }
            }

            // Se estamos em modo de recuperação de falha e só apanhámos 1 mensagem,
            // ou se ainda não apanhámos nada, esperamos 2s e tentamos outra vez (para dar tempo à retransmissão!)
            if (maxTentativas > 3) {
                System.out.println("Zoho: Polling aguarda retransmissoes... (Tentativa " + (i+1) + " de " + maxTentativas + ")");
            }
            Thread.sleep(2000);
        }

        return lastKnownList; // Devolve o que tiver conseguido ao fim das tentativas todas
    }

    // CORREÇÃO 6: Removido o synchronized!
    public Message getMessage(String mid) throws Exception {
        if (deletedMids.contains(mid)) return null;

        if (!isCleanStateInitialized) initCleanState();
        if (this.accountId == null) fetchAccountInfo();

        String zohoMsgId = null;
        String folderId = null;

        for (int i = 0; i < 3; i++) {
            OAuth2AccessToken accessToken = new OAuth2AccessToken(tokenManager.getValidAccessToken());
            OAuthRequest listReq = new OAuthRequest(Verb.GET, MAIL_API_BASE + "/accounts/" + accountId + "/messages/view");
            service.signRequest(accessToken, listReq);

            try (Response response = service.execute(listReq)) {
                if (response.isSuccessful()) {
                    ZohoMessageListReply reply = gson.fromJson(response.getBody(), ZohoMessageListReply.class);
                    if (reply != null && reply.data != null) {
                        for (ZohoMessageData m : reply.data) {
                            if (!ignoredZohoIds.contains(m.messageId) && m.subject != null && m.subject.startsWith(mid + "|to:")) {
                                zohoMsgId = m.messageId;
                                folderId = m.folderId;
                                break;
                            }
                        }
                    }
                }
            }
            if (zohoMsgId != null) break;
            Thread.sleep(2000);
        }

        if (zohoMsgId == null || folderId == null) return null;

        OAuth2AccessToken accessToken = new OAuth2AccessToken(tokenManager.getValidAccessToken());
        OAuthRequest msgReq = new OAuthRequest(Verb.GET,
                MAIL_API_BASE + "/accounts/" + accountId + "/folders/" + folderId + "/messages/" + zohoMsgId + "/content");
        service.signRequest(accessToken, msgReq);

        try (Response response = service.execute(msgReq)) {
            if (response.isSuccessful()) {
                ZohoSingleMessageReply reply = gson.fromJson(response.getBody(), ZohoSingleMessageReply.class);
                if (reply != null && reply.data != null && reply.data.content != null) {
                    String[] parts = reply.data.content.split("------");
                    if (parts.length > 1) {
                        String base64Json = parts[parts.length - 1].replaceAll("<[^>]*>", "").trim();
                        try {
                            String jsonMsg = new String(Base64.getDecoder().decode(base64Json));
                            return gson.fromJson(jsonMsg, Message.class);
                        } catch (Exception e) {}
                    }
                }
            }
        }
        return null;
    }

    class ZohoAccountReply { List<ZohoAccount> data; }
    class ZohoAccount { String accountId; String incomingUserName; }
    record ZohoSendEmailRequest(String fromAddress, String toAddress, String subject, String content) {}
    class ZohoMessageListReply { List<ZohoMessageData> data; }
    class ZohoMessageData { String messageId; String subject; String folderId; }
    class ZohoSingleMessageReply { ZohoSingleMessageData data; }
    class ZohoSingleMessageData { String content; }
}