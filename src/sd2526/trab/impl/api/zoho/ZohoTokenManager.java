package sd2526.trab.impl.api.zoho;

import com.github.scribejava.core.model.OAuth2AccessToken;
import com.github.scribejava.core.oauth.OAuth20Service;

public class ZohoTokenManager {
    private final OAuth20Service service;
    private final String refreshToken;

    // Variáveis para fazer cache do token
    private OAuth2AccessToken currentToken = null;
    private long expirationTimeMillis = 0;

    public ZohoTokenManager(OAuth20Service service, String refreshToken) {
        this.service = service;
        this.refreshToken = refreshToken;
    }

    public synchronized String getValidAccessToken() throws Exception {
        long now = System.currentTimeMillis();

        // Se não temos token, ou se expira em menos de 5 minutos (300000 milissegundos), renovamos.
        if (currentToken == null || now > (expirationTimeMillis - 300000)) {
            System.out.println("ZohoTokenManager: A renovar Access Token (o anterior expirou ou nao existia)...");

            // Pede novo token usando o Refresh Token
            currentToken = service.refreshAccessToken(refreshToken);

            // O Zoho costuma devolver um tempo de vida de 3600 segundos (1 hora)
            Integer expiresInSeconds = currentToken.getExpiresIn();
            if (expiresInSeconds == null) {
                expiresInSeconds = 3600; // fallback de segurança
            }

            // Calcular quando é que este novo token expira
            expirationTimeMillis = now + (expiresInSeconds * 1000L);
        }

        // Devolve o token guardado
        return currentToken.getAccessToken();
    }
}