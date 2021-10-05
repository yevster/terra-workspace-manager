package bio.terra.workspace.service.resource.controlled.azure;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ControlledAzureStorageContainerAttributes {
    private final String containerName;
    private final String storageAccountName;
    private final String azureEnvironment;

    @JsonCreator
    public ControlledAzureStorageContainerAttributes(
            @JsonProperty("storageAccount") String storageAccountName,
            @JsonProperty("container") String containerName,
            @JsonProperty("azureEnvironment") String azureEnvironment) {
        this.storageAccountName = storageAccountName;
        this.containerName = containerName;
        this.azureEnvironment = azureEnvironment;
    }

    public String getContainerName() {
        return containerName;
    }

    public String getStorageAccountName() {
        return storageAccountName;
    }

    public String getAzureEnvironment() {
        return azureEnvironment;
    }
}
