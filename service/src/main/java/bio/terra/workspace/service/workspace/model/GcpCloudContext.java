package bio.terra.workspace.service.workspace.model;

import bio.terra.workspace.db.DbSerDes;
import bio.terra.workspace.generated.model.ApiGcpContext;
import bio.terra.workspace.service.workspace.exceptions.InvalidSerializedVersionException;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;

public class GcpCloudContext {
  /** Serializing format of the GCP cloud context */
  @VisibleForTesting public static final long GCP_CLOUD_CONTEXT_DB_VERSION = 1;

  private final String gcpProjectId;

  @JsonCreator
  public GcpCloudContext(@JsonProperty String gcpProjectId) {
    this.gcpProjectId = gcpProjectId;
  }

  public String getGcpProjectId() {
    return gcpProjectId;
  }

  public ApiGcpContext toApi() {
    return new ApiGcpContext().projectId(getGcpProjectId());
  }

  // -- serdes for the GcpCloudContext --

  public String serialize() {
    GcpCloudContextV1 dbContext = GcpCloudContextV1.from(getGcpProjectId());
    return DbSerDes.toJson(dbContext);
  }

  public static GcpCloudContext deserialize(String json) {
    GcpCloudContextV1 result = DbSerDes.fromJson(json, GcpCloudContextV1.class);
    if (result.version != GCP_CLOUD_CONTEXT_DB_VERSION) {
      throw new InvalidSerializedVersionException("Invalid serialized version");
    }
    return new GcpCloudContext(result.gcpProjectId);
  }

  @VisibleForTesting
  public static class GcpCloudContextV1 {
    /** Version marker to store in the db so that we can update the format later if we need to. */
    @JsonProperty public final long version = GCP_CLOUD_CONTEXT_DB_VERSION;

    @JsonProperty public String gcpProjectId;

    public static GcpCloudContextV1 from(String googleProjectId) {
      GcpCloudContextV1 result = new GcpCloudContextV1();
      result.gcpProjectId = googleProjectId;
      return result;
    }
  }
}
