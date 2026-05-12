package sd2526.trab.impl.api.zoho;

import com.github.scribejava.core.builder.api.DefaultApi20;

public class ZohoApi extends DefaultApi20 {
    private static class InstanceHolder {
        private static final ZohoApi INSTANCE = new ZohoApi();
    }

    public static ZohoApi instance() {
        return InstanceHolder.INSTANCE;
    }

    @Override
    public String getAccessTokenEndpoint() {
        return "https://accounts.zoho.eu/oauth/v2/token";
    }

    @Override
    protected String getAuthorizationBaseUrl() {
        return "https://accounts.zoho.eu/oauth/v2/auth";
    }
}