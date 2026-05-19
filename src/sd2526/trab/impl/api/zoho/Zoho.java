package sd2526.trab.impl.api.zoho;

import java.util.Base64;
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

    // Conjuntos para o Estado Limpo e Apagar Virtual
    private final java.util.Set<String> ignoredZohoIds = new java.util.HashSet<>();
    private final java.util.Set<String> deletedMids = new java.util.HashSet<>();
    private boolean isCleanStateInitialized = false;

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
        isCleanStateInitialized = true;
    }

    public synchronized void deleteMessage(String mid) {
        deletedMids.add(mid);
        System.out.println("Zoho: Mensagem " + mid + " apagada virtualmente.");
    }

    public synchronized String sendEmail(Message msg) throws Exception {
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

    public synchronized List<String> getAllMessages(String expectedRecipient) throws Exception {
        if (!isCleanStateInitialized) initCleanState();
        if (this.accountId == null || this.myEmailAddress == null) fetchAccountInfo();
        if (this.accountId == null) return List.of();

        for (int i = 0; i < 3; i++) {
            OAuth2AccessToken accessToken = new OAuth2AccessToken(tokenManager.getValidAccessToken());
            OAuthRequest request = new OAuthRequest(Verb.GET, MAIL_API_BASE + "/accounts/" + accountId + "/messages/view");
            service.signRequest(accessToken, request);

            try (Response response = service.execute(request)) {
                if(response.isSuccessful()) {
                    ZohoMessageListReply reply = gson.fromJson(response.getBody(), ZohoMessageListReply.class);
                    if (reply != null && reply.data != null && !reply.data.isEmpty()) {
                        return reply.data.stream()
                                .filter(m -> !ignoredZohoIds.contains(m.messageId)) // Esconde lixo antigo
                                .filter(m -> m.subject != null && m.subject.contains("|to:" + expectedRecipient))
                                .map(m -> m.subject.split("\\|to:")[0])
                                .filter(mid -> !deletedMids.contains(mid)) // Esconde as apagadas
                                .toList();
                    }
                }
            }
            Thread.sleep(2000);
        }
        return List.of();
    }

    public synchronized Message getMessage(String mid) throws Exception {
        if (deletedMids.contains(mid)) return null; // Se foi apagada virtualmente

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