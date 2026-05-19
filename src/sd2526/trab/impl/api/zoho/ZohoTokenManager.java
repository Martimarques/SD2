package sd2526.trab.impl.api.zoho;

import com.github.scribejava.core.model.OAuth2AccessToken;
import com.github.scribejava.core.oauth.OAuth20Service;

public class ZohoTokenManager {
    private final OAuth20Service service;
    private final String refreshToken;
    private OAuth2AccessToken accessToken;

    public ZohoTokenManager(OAuth20Service service, String refreshToken) {
        this.service = service;
        this.refreshToken = refreshToken;
    }

    public synchronized String getValidAccessToken() throws Exception {
        if (accessToken == null || System.currentTimeMillis() >= (accessToken.getExpiresIn() * 1000L)) {
            accessToken = service.refreshAccessToken(refreshToken);
        }
        return accessToken.getAccessToken();
    }
}