package bio.terra.workspace.db.gcp;

import bio.terra.workspace.service.resource.WsmResourceType;
import bio.terra.workspace.service.resource.controlled.ControlledResource;
import bio.terra.workspace.service.resource.controlled.gcp.ControlledAiNotebookInstanceResource;
import bio.terra.workspace.service.resource.controlled.gcp.ControlledBigQueryDatasetResource;
import bio.terra.workspace.service.resource.controlled.gcp.ControlledGcsBucketResource;
import bio.terra.workspace.service.resource.exception.DuplicateResourceException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

@Component
public class GcpResourceUniquenessValidationDao {
  private final NamedParameterJdbcTemplate jdbcTemplate;

  @Autowired
  public GcpResourceUniquenessValidationDao(NamedParameterJdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

  public void validateUnique(ControlledResource controlledResource) {
    switch (controlledResource.getResourceType()) {
      case GCS_BUCKET:
        validateUniqueGcsBucket(ControlledGcsBucketResource.cast(controlledResource));
        break;
      case AI_NOTEBOOK_INSTANCE:
        validateUniqueAiNotebookInstance(
            ControlledAiNotebookInstanceResource.cast(controlledResource));
        break;
      case BIG_QUERY_DATASET:
        validateUniqueBigQueryDataset(ControlledBigQueryDatasetResource.cast(controlledResource));
        break;
      case DATA_REPO_SNAPSHOT:
      default:
        throw new IllegalArgumentException(
            String.format(
                "Resource type %s not supported", controlledResource.getResourceType().toString()));
    }
  }

  private void validateUniqueGcsBucket(ControlledGcsBucketResource bucketResource) {
    String bucketSql =
        "SELECT COUNT(1)"
            + " FROM resource"
            + " WHERE resource_type = :resource_type"
            + " AND attributes->>'bucketName' = :bucket_name";
    MapSqlParameterSource bucketParams =
        new MapSqlParameterSource()
            .addValue("bucket_name", bucketResource.getBucketName())
            .addValue("resource_type", WsmResourceType.GCS_BUCKET.toSql());
    Integer matchingBucketCount =
        jdbcTemplate.queryForObject(bucketSql, bucketParams, Integer.class);
    if (matchingBucketCount != null && matchingBucketCount > 0) {
      throw new DuplicateResourceException(
          String.format(
              "A GCS bucket resource named %s already exists", bucketResource.getBucketName()));
    }
  }

  private void validateUniqueAiNotebookInstance(
      ControlledAiNotebookInstanceResource notebookResource) {
    // Workspace ID is a proxy for project ID, which works because there is a permanent, 1:1
    // correspondence between workspaces and GCP projects.
    String sql =
        "SELECT COUNT(1)"
            + " FROM resource"
            + " WHERE resource_type = :resource_type"
            + " AND workspace_id = :workspace_id"
            + " AND attributes->>'instanceId' = :instance_id"
            + " AND attributes->>'location' = :location";
    MapSqlParameterSource sqlParams =
        new MapSqlParameterSource()
            .addValue("resource_type", WsmResourceType.AI_NOTEBOOK_INSTANCE.toSql())
            .addValue("workspace_id", notebookResource.getWorkspaceId().toString())
            .addValue("instance_id", notebookResource.getInstanceId())
            .addValue("location", notebookResource.getLocation());
    Integer matchingCount = jdbcTemplate.queryForObject(sql, sqlParams, Integer.class);
    if (matchingCount != null && matchingCount > 0) {
      throw new DuplicateResourceException(
          String.format(
              "An AI Notebook instance with ID %s already exists",
              notebookResource.getInstanceId()));
    }
  }

  private void validateUniqueBigQueryDataset(ControlledBigQueryDatasetResource datasetResource) {
    // Workspace ID is a proxy for project ID, which works because there is a permanent, 1:1
    // correspondence between workspaces and GCP projects.
    String sql =
        "SELECT COUNT(1)"
            + " FROM resource"
            + " WHERE resource_type = :resource_type"
            + " AND workspace_id = :workspace_id"
            + " AND attributes->>'datasetName' = :dataset_name";
    MapSqlParameterSource sqlParams =
        new MapSqlParameterSource()
            .addValue("resource_type", WsmResourceType.BIG_QUERY_DATASET.toSql())
            .addValue("workspace_id", datasetResource.getWorkspaceId().toString())
            .addValue("dataset_name", datasetResource.getDatasetName());
    Integer matchingCount = jdbcTemplate.queryForObject(sql, sqlParams, Integer.class);
    if (matchingCount != null && matchingCount > 0) {
      throw new DuplicateResourceException(
          String.format(
              "A BigQuery dataset with ID %s already exists", datasetResource.getDatasetName()));
    }
  }
}
