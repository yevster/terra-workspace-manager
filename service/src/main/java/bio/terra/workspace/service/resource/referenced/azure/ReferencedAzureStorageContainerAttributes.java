package bio.terra.workspace.service.resource.referenced.azure;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ReferencedAzureStorageContainerAttributes {
  private final String containerName;

  @JsonCreator
  public ReferencedAzureStorageContainerAttributes(
      @JsonProperty("containerName") String containerName) {
    this.containerName = containerName;
  }

  public String getBucketName() {
    return containerName;
  }
}
