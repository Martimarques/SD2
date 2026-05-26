package sd2526.trab.impl.api.zoho;

import com.github.scribejava.core.model.OAuth2AccessToken;
import com.github.scribejava.core.oauth.OAuth20Service;

public class ZohoTokenManager {
    private final OAuth20Service service;
    private final String refreshToken;

    private OAuth2AccessToken currentToken = null;
    private long expirationTimeMillis = 0;

    public ZohoTokenManager(OAuth20Service service, String refreshToken) {
        this.service = service;
        this.refreshToken = refreshToken;
    }

    public synchronized String getValidAccessToken() throws Exception {
        long now = System.currentTimeMillis();

        if (currentToken == null || now > (expirationTimeMillis - 300000)) {
            System.out.println("ZohoTokenManager: A renovar Access Token (o anterior expirou ou nao existia)...");

            currentToken = service.refreshAccessToken(refreshToken);

            Integer expiresInSeconds = currentToken.getExpiresIn();
            if (expiresInSeconds == null) {
                expiresInSeconds = 3600;
            }

            expirationTimeMillis = now + (expiresInSeconds * 1000L);
        }

        return currentToken.getAccessToken();
    }
}