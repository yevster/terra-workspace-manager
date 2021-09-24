package bio.terra.workspace.service.resource.controlled.azure;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ControlledAzureStorageContainerAttributes {
  private final String containerName;

  @JsonCreator
  public ControlledAzureStorageContainerAttributes(@JsonProperty("container") String containerName) {
    this.containerName = containerName;
  }

  public String getContainerName() {
    return containerName;
  }
}
