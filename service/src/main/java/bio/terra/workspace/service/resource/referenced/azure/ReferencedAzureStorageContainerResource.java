package bio.terra.workspace.service.resource.referenced.azure;

import bio.terra.common.exception.InconsistentFieldsException;
import bio.terra.common.exception.MissingRequiredFieldException;
import bio.terra.workspace.common.utils.FlightBeanBag;
import bio.terra.workspace.db.DbSerDes;
import bio.terra.workspace.db.model.DbResource;
import bio.terra.workspace.generated.model.ApiGcpGcsBucketAttributes;
import bio.terra.workspace.generated.model.ApiGcpGcsBucketResource;
import bio.terra.workspace.service.crl.CrlService;
import bio.terra.workspace.service.iam.AuthenticatedUserRequest;
import bio.terra.workspace.service.resource.ValidationUtils;
import bio.terra.workspace.service.resource.WsmResourceType;
import bio.terra.workspace.service.resource.model.CloningInstructions;
import bio.terra.workspace.service.resource.referenced.ReferencedResource;
import bio.terra.workspace.service.resource.referenced.gcp.ReferencedGcsBucketAttributes;
import bio.terra.workspace.service.resource.referenced.gcp.ReferencedGcsBucketResource;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Strings;
import java.util.Optional;
import java.util.UUID;

public class ReferencedAzureStorageContainerResource extends ReferencedResource {
  private final String containerName;

  /**
   * Constructor for serialized form for Stairway use and used by the builder
   *
   * @param workspaceId workspace unique identifier
   * @param resourceId resource unique identifier
   * @param name name - may be null
   * @param description description - may be null
   * @param cloningInstructions cloning instructions
   * @param containerName container name
   */
  @JsonCreator
  public ReferencedAzureStorageContainerResource(
      @JsonProperty("workspaceId") UUID workspaceId,
      @JsonProperty("resourceId") UUID resourceId,
      @JsonProperty("name") String name,
      @JsonProperty("description") String description,
      @JsonProperty("cloningInstructions") CloningInstructions cloningInstructions,
      @JsonProperty("containerName") String containerName) {
    super(workspaceId, resourceId, name, description, cloningInstructions);
    this.containerName = containerName;
    validate();
  }

  /**
   * Constructor from database metadata
   *
   * @param dbResource database form of resources
   */
  public ReferencedAzureStorageContainerResource(DbResource dbResource) {
    super(dbResource);
    ReferencedAzureStorageContainerResource attributes =
        DbSerDes.fromJson(
            dbResource.getAttributes(), ReferencedAzureStorageContainerResource.class);
    this.containerName = attributes.getContainerName();
    validate();
  }

  public String getContainerName() {
    return containerName;

    AzureResourceManager
  }

  public ApiAzureStorageContainerAttributes toApiAttributes() {
    return new ApiAzureStorageContainerAttributes().containerName(getContainerName());
  }

  public ApiGcpGcsBucketResource toApiModel() {
    return new ApiGcpGcsBucketResource()
        .metadata(super.toApiMetadata())
        .attributes(toApiAttributes());
  }

  @Override
  public WsmResourceType getResourceType() {
    return WsmResourceType.GCS_BUCKET;
  }

  @Override
  public String attributesToJson() {
    return DbSerDes.toJson(new ReferencedGcsBucketAttributes(bucketName));
  }

  @Override
  public void validate() {
    super.validate();
    if (getResourceType() != WsmResourceType.GCS_BUCKET) {
      throw new InconsistentFieldsException("Expected GCS_BUCKET");
    }
    if (Strings.isNullOrEmpty(getBucketName())) {
      throw new MissingRequiredFieldException("Missing required field for ReferenceGcsBucket.");
    }
    ValidationUtils.validateBucketName(getBucketName());
  }

  @Override
  public boolean checkAccess(FlightBeanBag context, AuthenticatedUserRequest userRequest) {
    CrlService crlService = context.getCrlService();
    return crlService.canReadGcsBucket(bucketName, userRequest);
  }

  /**
   * Make a copy of this object via a new builder. This is convenient for reusing objects with one
   * or two fields changed.
   *
   * @return builder object ready for new values to replace existing ones
   */
  public ReferencedGcsBucketResource.Builder toBuilder() {
    return builder()
        .bucketName(getBucketName())
        .cloningInstructions(getCloningInstructions())
        .description(getDescription())
        .name(getName())
        .resourceId(getResourceId())
        .workspaceId(getWorkspaceId());
  }

  public static ReferencedGcsBucketResource.Builder builder() {
    return new ReferencedGcsBucketResource.Builder();
  }

  public static class Builder {
    private CloningInstructions cloningInstructions;
    private String bucketName;
    private String description;
    private String name;
    private UUID resourceId;
    private UUID workspaceId;

    public ReferencedGcsBucketResource.Builder workspaceId(UUID workspaceId) {
      this.workspaceId = workspaceId;
      return this;
    }

    public ReferencedGcsBucketResource.Builder resourceId(UUID resourceId) {
      this.resourceId = resourceId;
      return this;
    }

    public ReferencedGcsBucketResource.Builder name(String name) {
      this.name = name;
      return this;
    }

    public ReferencedGcsBucketResource.Builder description(String description) {
      this.description = description;
      return this;
    }

    public ReferencedGcsBucketResource.Builder cloningInstructions(
        CloningInstructions cloningInstructions) {
      this.cloningInstructions = cloningInstructions;
      return this;
    }

    public ReferencedGcsBucketResource.Builder bucketName(String bucketName) {
      this.bucketName = bucketName;
      return this;
    }

    public ReferencedGcsBucketResource build() {
      // On the create path, we can omit the resourceId and have it filled in by the builder.
      return new ReferencedGcsBucketResource(
          workspaceId,
          Optional.ofNullable(resourceId).orElse(UUID.randomUUID()),
          name,
          description,
          cloningInstructions,
          bucketName);
    }
  }
}
