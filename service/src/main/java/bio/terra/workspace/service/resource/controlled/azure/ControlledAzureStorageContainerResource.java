package bio.terra.workspace.service.resource.controlled.azure;

import bio.terra.common.exception.InconsistentFieldsException;
import bio.terra.common.exception.MissingRequiredFieldException;
import bio.terra.workspace.db.DbSerDes;
import bio.terra.workspace.db.model.DbResource;
import bio.terra.workspace.generated.model.ApiAzureStorageContainerAttributes;
import bio.terra.workspace.generated.model.ApiAzureStorageContainerResource;
import bio.terra.workspace.generated.model.ApiGcpGcsBucketAttributes;
import bio.terra.workspace.generated.model.ApiGcpGcsBucketResource;
import bio.terra.workspace.service.resource.ValidationUtils;
import bio.terra.workspace.service.resource.WsmResourceType;
import bio.terra.workspace.service.resource.controlled.AccessScopeType;
import bio.terra.workspace.service.resource.controlled.ControlledResource;
import bio.terra.workspace.service.resource.controlled.ManagedByType;
import bio.terra.workspace.service.resource.controlled.flight.create.azure.AzureConfigurationUtilities;
import bio.terra.workspace.service.resource.model.CloningInstructions;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.UUID;

public class ControlledAzureStorageContainerResource extends ControlledResource {
  private final String containerName;
  private final String storageAccountName;

  @JsonCreator
  public ControlledAzureStorageContainerResource(
      @JsonProperty("workspaceId") UUID workspaceId,
      @JsonProperty("resourceId") UUID resourceId,
      @JsonProperty("name") String name,
      @JsonProperty("description") String description,
      @JsonProperty("cloningInstructions") CloningInstructions cloningInstructions,
      @JsonProperty("assignedUser") String assignedUser,
      @JsonProperty("accessScope") AccessScopeType accessScope,
      @JsonProperty("managedBy") ManagedByType managedBy,
      @JsonProperty("storageAccountName") String storageAccountName,
      @JsonProperty("containerName") String containerName) {

    super(
        workspaceId,
        resourceId,
        name,
        description,
        cloningInstructions,
        assignedUser,
        accessScope,
        managedBy);
    this.storageAccountName = storageAccountName;
    this.containerName = containerName;
    validate();
  }

  public ControlledAzureStorageContainerResource(DbResource dbResource) {
    super(dbResource);
    ControlledAzureStorageContainerAttributes attributes =
        DbSerDes.fromJson(
            dbResource.getAttributes(), ControlledAzureStorageContainerAttributes.class);
    this.containerName = attributes.getContainerName();
    this.storageAccountName = attributes.getStorageAccountName();
    validate();
  }

  public static Builder builder() {
    return new Builder();
  }

  public String getContainerName() {
    return containerName;
  }

  public String getStorageAccountName() { return storageAccountName; }

  public ApiAzureStorageContainerAttributes toApiAttributes() {
    return new ApiAzureStorageContainerAttributes()
            .containerName(getContainerName())
            .storageAccountName(getStorageAccountName());
  }

  public ApiAzureStorageContainerResource toApiResource() {
    return new ApiAzureStorageContainerResource()
        .metadata(super.toApiMetadata())
        .attributes(toApiAttributes());
  }

  @Override
  public WsmResourceType getResourceType() {
    return WsmResourceType.AZURE_STORAGE_CONTAINER;
  }

  @Override
  public String attributesToJson() {
    return DbSerDes.toJson(new ControlledAzureStorageContainerAttributes(getStorageAccountName(), getContainerName()));
  }

  @Override
  public void validate() {
    super.validate();
    if (getResourceType() != WsmResourceType.GCS_BUCKET) {
      throw new InconsistentFieldsException("Expected GCS_BUCKET");
    }
    if (getContainerName() == null) {
      throw new MissingRequiredFieldException("Missing required field for ControlledGcsBucket.");
    }
    ValidationUtils.validateBucketName(getContainerName());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;

    ControlledAzureStorageContainerResource that = (ControlledAzureStorageContainerResource) o;

    return containerName.equals(that.containerName);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + containerName.hashCode();
    return result;
  }

  public static class Builder {
    private UUID workspaceId;
    private UUID resourceId;
    private String name;
    private String description;
    private CloningInstructions cloningInstructions;
    private String assignedUser;
    private AccessScopeType accessScope;
    private ManagedByType managedBy;
    private String containerName;
    private String storageAccountName;
    private String azureEnvironment = "AZURE";

    public Builder workspaceId(UUID workspaceId) {
      this.workspaceId = workspaceId;
      return this;
    }

    public Builder resourceId(UUID resourceId) {
      this.resourceId = resourceId;
      return this;
    }

    public Builder name(String name) {
      this.name = name;
      return this;
    }

    public Builder description(String description) {
      this.description = description;
      return this;
    }

    public Builder cloningInstructions(CloningInstructions cloningInstructions) {
      this.cloningInstructions = cloningInstructions;
      return this;
    }

    public Builder containerName(String containerName) {
      this.containerName = containerName;
      return this;
    }

    public Builder storageAccountName(String storageAccountName) {
      this.storageAccountName = storageAccountName;
      return this;
    }

    public Builder assignedUser(String assignedUser) {
      this.assignedUser = assignedUser;
      return this;
    }

    public Builder accessScope(AccessScopeType accessScope) {
      this.accessScope = accessScope;
      return this;
    }

    public Builder managedBy(ManagedByType managedBy) {
      this.managedBy = managedBy;
      return this;
    }

    public ControlledAzureStorageContainerResource build() {
      return new ControlledAzureStorageContainerResource(
          workspaceId,
          resourceId,
          name,
          description,
          cloningInstructions,
          assignedUser,
          accessScope,
          managedBy,
          storageAccountName,
          containerName);
    }
  }

  // Double-checked down casts when we need to re-specialize from a ControlledResource
  public static ControlledAzureStorageContainerResource cast(ControlledResource resource) {
    validateSubclass(resource, WsmResourceType.AZURE_STORAGE_CONTAINER);
    return (ControlledAzureStorageContainerResource) resource;
  }
}
