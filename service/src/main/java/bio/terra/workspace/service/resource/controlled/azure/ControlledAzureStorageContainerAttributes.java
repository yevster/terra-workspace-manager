package bio.terra.workspace.service.resource.controlled.azure;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ControlledAzureStorageContainerAttributes {
    private final String containerName;
    private final String storageAccountName;

    @JsonCreator
    public ControlledAzureStorageContainerAttributes(
            @JsonProperty("storageAccount") String storageAccountName,
            @JsonProperty("container") String containerName) {
        this.storageAccountName = storageAccountName;
        this.containerName = containerName;
    }

    public String getContainerName() {
        return containerName;
    }

    public String getStorageAccountName() {
        return storageAccountName;
    }

}
