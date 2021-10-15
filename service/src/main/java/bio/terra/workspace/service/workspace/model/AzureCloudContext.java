package bio.terra.workspace.service.workspace.model;

import bio.terra.workspace.db.DbSerDes;
import bio.terra.workspace.generated.model.ApiAzureContext;
import bio.terra.workspace.service.workspace.exceptions.InvalidSerializedVersionException;
import com.azure.core.credential.TokenCredential;
import com.azure.core.management.AzureEnvironment;
import com.azure.core.management.profile.AzureProfile;
import com.azure.core.util.Configuration;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.resourcemanager.storage.StorageManager;
import com.azure.storage.blob.BlobClientBuilder;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Field;

public class AzureCloudContext {
    /**
     * Format version for serialized form of Azure cloud context - deliberately chosen to be different
     * from the GCP cloud context version.
     */
    @VisibleForTesting
    public static final long AZURE_CLOUD_CONTEXT_DB_VERSION = 100;

    private final String azureTenantId;
    private final String azureSubscriptionId;
    private final String azureResourceGroupId;
    private final String azureEnvironment;

    @JsonCreator
    public AzureCloudContext(
            @JsonProperty String azureTenantId,
            @JsonProperty String azureSubscriptionId,
            @JsonProperty String azureResourceGroupId) {
        this(azureTenantId, azureSubscriptionId, azureResourceGroupId, "AZURE");
    }

    @JsonCreator
    public AzureCloudContext(
            @JsonProperty String azureTenantId,
            @JsonProperty String azureSubscriptionId,
            @JsonProperty String azureResourceGroupId,
            @JsonProperty String azureEnvironment) {
        this.azureTenantId = azureTenantId;
        this.azureSubscriptionId = azureSubscriptionId;
        this.azureResourceGroupId = azureResourceGroupId;

        // Fail early if the Azure environment name is invalid.
        if (ApiAzureContext.AzureEnvironmentEnum.fromValue(azureEnvironment) == null) {
            throw new IllegalArgumentException("Invalid Azure Environment name: " + azureEnvironment);
        }
        this.azureEnvironment = azureEnvironment;
    }

    public String getAzureTenantId() {
        return azureTenantId;
    }

    public String getAzureSubscriptionId() {
        return azureSubscriptionId;
    }

    public String getAzureResourceGroupId() {
        return azureResourceGroupId;
    }

    public String getAzureEnvironment() {
        return azureEnvironment;
    }

    public ApiAzureContext toApi() {
        return new ApiAzureContext()
                .tenantId(getAzureTenantId())
                .subscriptionId(getAzureSubscriptionId())
                .resourceGroupId(getAzureResourceGroupId())
                .azureEnvironment(ApiAzureContext.AzureEnvironmentEnum.valueOf(getAzureEnvironment()));
    }

    public static AzureCloudContext fromApi(ApiAzureContext azureContext) {
        return new AzureCloudContext(
                azureContext.getTenantId(),
                azureContext.getSubscriptionId(),
                azureContext.getResourceGroupId(),
                azureContext.getAzureEnvironment().name());
    }

    // -- serdes for the GcpCloudContext --

    public String serialize() {
        AzureCloudContextV100 dbContext =
                AzureCloudContextV100.from(
                        getAzureTenantId(),
                        getAzureSubscriptionId(),
                        getAzureResourceGroupId(),
                        getAzureEnvironment());
        return DbSerDes.toJson(dbContext);
    }

    public static AzureCloudContext deserialize(String json) {
        AzureCloudContextV100 result = DbSerDes.fromJson(json, AzureCloudContextV100.class);
        if (result.version != AZURE_CLOUD_CONTEXT_DB_VERSION) {
            throw new InvalidSerializedVersionException("Invalid serialized version");
        }
        return new AzureCloudContext(
                result.azureTenantId, result.azureSubscriptionId, result.azureResourceGroupId);
    }

    @VisibleForTesting
    public static class AzureCloudContextV100 {
        /**
         * Version marker to store in the db so that we can update the format later if we need to.
         */
        @JsonProperty
        public final long version = AZURE_CLOUD_CONTEXT_DB_VERSION;

        @JsonProperty
        public String azureTenantId;
        @JsonProperty
        public String azureSubscriptionId;
        @JsonProperty
        public String azureResourceGroupId;
        @JsonProperty
        public String azureEnvironment;

        public static AzureCloudContextV100 from(
                String tenantId, String subscriptionId, String resourceGroupId, String azureEnvironment) {
            AzureCloudContextV100 result = new AzureCloudContextV100();
            result.azureTenantId = tenantId;
            result.azureSubscriptionId = subscriptionId;
            result.azureResourceGroupId = resourceGroupId;
            result.azureEnvironment = azureEnvironment;
            return result;
        }
    }

    private static AzureEnvironment getAzureEnvironmentByName(String azureEnvironment) {
        if (StringUtils.isBlank(azureEnvironment)) return AzureEnvironment.AZURE;
        try {
            Field environmentConstant = AzureEnvironment.class.getDeclaredField(azureEnvironment);
            return (AzureEnvironment) environmentConstant.get(null);
        } catch (NoSuchFieldException nsfe) {
            throw new IllegalArgumentException("Unable to find Azure environment: " + azureEnvironment);
        } catch (IllegalAccessException iae) { // Should never happen unless SDK changes
            throw new IllegalStateException("Unable to access available azure environment names");
        }
    }

    private TokenCredential getCredential() {
        return new DefaultAzureCredentialBuilder().tenantId(this.getAzureTenantId()).build();
    }

    @JsonIgnore
    public StorageManager getAzureStorageManager() {
        TokenCredential azureCredential = getCredential();

        // Does the storage account exist? If not create it.
        AzureEnvironment azureEnvironment =
                getAzureEnvironmentByName(this.getAzureEnvironment());

        return StorageManager.authenticate(
                azureCredential,
                new AzureProfile(
                        this.getAzureTenantId(),
                        this.getAzureSubscriptionId(),
                        azureEnvironment));
    }

    @JsonIgnore
    public BlobContainerClientBuilder authenticatedBlobContainerClientBuilder(){
        return new BlobContainerClientBuilder().credential(getCredential());

    }

}
