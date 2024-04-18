package com.twogenidentity.keycloak.service;

import com.twogenidentity.keycloak.event.EventParser;
import com.twogenidentity.keycloak.utils.OpenFgaHelper;
import dev.openfga.sdk.api.client.OpenFgaClient;
import dev.openfga.sdk.api.client.model.ClientWriteRequest;
import dev.openfga.sdk.api.configuration.ClientConfiguration;
import dev.openfga.sdk.api.configuration.ClientWriteOptions;
import dev.openfga.sdk.api.model.AuthorizationModel;
import dev.openfga.sdk.api.model.ListStoresResponse;
import dev.openfga.sdk.api.model.ReadAuthorizationModelsResponse;
import dev.openfga.sdk.api.model.Store;
import dev.openfga.sdk.errors.FgaInvalidParameterException;
import org.jboss.logging.Logger;
import org.keycloak.Config;
import org.keycloak.utils.StringUtil;

import java.time.Duration;
import java.util.concurrent.ExecutionException;

public class OpenFgaClientHandler {

    protected final Config.Scope config;
    private final OpenFgaClient fgaClient;
    private final OpenFgaHelper fgaHelper;
    private boolean clientInitialized;

    private final ClientWriteOptions clientWriteOptions;

    protected static final String OPENFGA_API_URL = "openfgaApiUrl";
    protected static final String OPENFGA_STORE_ID = "openfgaStoreId";
    protected static final String OPENFGA_AUTHORIZATION_MODEL_ID = "openfgaAuthorizationModelId";

    private static final Logger LOG = Logger.getLogger(OpenFgaClientHandler.class);

    public OpenFgaClientHandler(Config.Scope config) throws FgaInvalidParameterException {
        this.config = config;

        ClientConfiguration configuration = new ClientConfiguration().apiUrl(getOpenFgaApiUrl()).connectTimeout(Duration.ofSeconds(5)).readTimeout(Duration.ofSeconds(5));

        if (StringUtil.isNotBlank(getOpenFgaOpenStoreId()) && StringUtil.isNotBlank(getOpenFgaAuthorizationModelId())) {
            configuration.storeId(getOpenFgaOpenStoreId());
            configuration.authorizationModelId(getOpenFgaAuthorizationModelId());
            this.clientInitialized = true;
        }

        this.fgaHelper = new OpenFgaHelper();
        this.clientWriteOptions = new ClientWriteOptions();
        this.fgaClient = new OpenFgaClient(configuration);
    }

    public void publish(String eventId, EventParser event) throws FgaInvalidParameterException, ExecutionException, InterruptedException {
        if (!this.clientInitialized && !this.discoverClientConfiguration()) {
            LOG.errorf("Unable to initialized OpenFga client. Discarding  event %s, %s", eventId, event.toString());
        } else {
            ClientWriteRequest request = fgaHelper.toClientWriteRequest(event);
            if (fgaHelper.isAvailableClientRequest(request)) {
                LOG.debugf("Publishing event id %s", eventId);
                var response = fgaClient.write(request, this.clientWriteOptions).get();
                LOG.debugf("Successfully sent tuple key to OpenFga, response: %s", response);
            }
        }
    }

    private boolean discoverClientConfiguration() throws FgaInvalidParameterException, ExecutionException, InterruptedException {
        LOG.info("Discover store and authorization model");
        ListStoresResponse stores = fgaClient.listStores().get();
        if (!stores.getStores().isEmpty()) {
            Store store = stores.getStores().get(0);
            LOG.infof("Found store id: %s", store.getId());
            this.fgaClient.setStoreId(store.getId());
            ReadAuthorizationModelsResponse authorizationModels = fgaClient.readAuthorizationModels().get();
            if (!authorizationModels.getAuthorizationModels().isEmpty()) {
                AuthorizationModel model = authorizationModels.getAuthorizationModels().get(0);
                LOG.infof("Found authorization model id: %s", model.getId());
                this.fgaClient.setAuthorizationModelId(model.getId());
                this.fgaHelper.loadModel(model);
                this.clientInitialized = true;
            }
        }
        return this.clientInitialized;
    }

    public String getOpenFgaApiUrl() {
        return config.get(OPENFGA_API_URL) != null ? config.get(OPENFGA_API_URL) : "http://openfga:8080";
    }

    public String getOpenFgaOpenStoreId() {
        return config.get(OPENFGA_STORE_ID) != null ? config.get(OPENFGA_STORE_ID) : "";
    }

    public String getOpenFgaAuthorizationModelId() {
        return config.get(OPENFGA_AUTHORIZATION_MODEL_ID) != null ? config.get(OPENFGA_AUTHORIZATION_MODEL_ID) : "";
    }
}
