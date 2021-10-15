package bio.terra.workspace.service.resource.controlled.gcp;

import bio.terra.common.exception.BadRequestException;
import bio.terra.workspace.db.ResourceDao;
import bio.terra.workspace.generated.model.*;
import bio.terra.workspace.service.iam.AuthenticatedUserRequest;
import bio.terra.workspace.service.iam.SamService;
import bio.terra.workspace.service.iam.model.ControlledResourceIamRole;
import bio.terra.workspace.service.iam.model.SamConstants;
import bio.terra.workspace.service.job.JobBuilder;
import bio.terra.workspace.service.job.JobService;
import bio.terra.workspace.service.resource.controlled.ControlledResource;
import bio.terra.workspace.service.resource.controlled.ControlledResourceMetadataManager;
import bio.terra.workspace.service.resource.controlled.ControlledResourceService;
import bio.terra.workspace.service.resource.controlled.flight.clone.bucket.CloneControlledGcsBucketResourceFlight;
import bio.terra.workspace.service.resource.controlled.flight.clone.dataset.CloneControlledGcpBigQueryDatasetResourceFlight;
import bio.terra.workspace.service.resource.controlled.flight.update.gcp.UpdateControlledGcsBucketResourceFlight;
import bio.terra.workspace.service.resource.controlled.flight.update.gcs.UpdateControlledBigQueryDatasetResourceFlight;
import bio.terra.workspace.service.resource.model.CloningInstructions;
import bio.terra.workspace.service.stage.StageService;
import bio.terra.workspace.service.workspace.WorkspaceService;
import bio.terra.workspace.service.workspace.flight.WorkspaceFlightMapKeys;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import javax.annotation.Nullable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ControlledGcpResourceService {
  private final JobService jobService;
  private final WorkspaceService workspaceService;
  private final ResourceDao resourceDao;
  private final StageService stageService;
  private final SamService samService;
  private final ControlledResourceMetadataManager controlledResourceMetadataManager;
  private final ControlledResourceService controlledResourceService;

  @Autowired
  public ControlledGcpResourceService(
      JobService jobService,
      WorkspaceService workspaceService,
      ResourceDao resourceDao,
      StageService stageService,
      SamService samService,
      ControlledResourceMetadataManager controlledResourceMetadataManager,
      ControlledResourceService controlledResourceService) {
    this.jobService = jobService;
    this.workspaceService = workspaceService;
    this.resourceDao = resourceDao;
    this.stageService = stageService;
    this.samService = samService;
    this.controlledResourceMetadataManager = controlledResourceMetadataManager;
    this.controlledResourceService = controlledResourceService;
  }

  /** Starts a create controlled bucket resource, blocking until its job is finished. */
  public ControlledGcsBucketResource createBucket(
      ControlledGcsBucketResource resource,
      ApiGcpGcsBucketCreationParameters creationParameters,
      List<ControlledResourceIamRole> privateResourceIamRoles,
      AuthenticatedUserRequest userRequest) {
    JobBuilder jobBuilder =
        controlledResourceService
            .commonCreationJobBuilder(
                resource,
                privateResourceIamRoles,
                new ApiJobControl().id(UUID.randomUUID().toString()),
                null,
                userRequest)
            .addParameter(
                WorkspaceFlightMapKeys.ControlledResourceKeys.CREATION_PARAMETERS,
                creationParameters);
    return jobBuilder.submitAndWait(ControlledGcsBucketResource.class);
  }

  public ControlledGcsBucketResource updateGcsBucket(
      ControlledGcsBucketResource resource,
      @Nullable ApiGcpGcsBucketUpdateParameters updateParameters,
      AuthenticatedUserRequest userRequest,
      @Nullable String resourceName,
      @Nullable String resourceDescription) {
    final String jobDescription =
        String.format(
            "Update controlled GCS Bucket resource %s; id %s; name %s",
            resource.getBucketName(), resource.getResourceId(), resource.getName());
    final JobBuilder jobBuilder =
        jobService
            .newJob(
                jobDescription,
                UUID.randomUUID().toString(), // no need to track ID
                UpdateControlledGcsBucketResourceFlight.class,
                resource,
                userRequest)
            .addParameter(
                WorkspaceFlightMapKeys.ControlledResourceKeys.UPDATE_PARAMETERS, updateParameters)
            .addParameter(WorkspaceFlightMapKeys.ControlledResourceKeys.RESOURCE_NAME, resourceName)
            .addParameter(
                WorkspaceFlightMapKeys.ControlledResourceKeys.RESOURCE_DESCRIPTION,
                resourceDescription);
    return jobBuilder.submitAndWait(ControlledGcsBucketResource.class);
  }

  /**
   * Clone a GCS Bucket to another workspace.
   *
   * @param sourceWorkspaceId - workspace ID fo source bucket
   * @param sourceResourceId - resource ID of source bucket
   * @param destinationWorkspaceId - workspace ID to clone into
   * @param jobControl - job service control structure
   * @param userRequest - incoming request
   * @param destinationResourceName - override value for resource name. Re-uses previous name if
   *     null
   * @param destinationDescription - override value for resource description. Re-uses previous value
   *     if null
   * @param destinationBucketName - GCS bucket name for cloned bucket. If null, a random name will
   *     be generated
   * @param destinationLocation - location string for the destination bucket. If null, the source
   *     bucket's location will be used.
   * @param cloningInstructionsOverride - cloning instructions for this operation. If null, the
   *     source bucket's cloning instructions will be honored.
   * @return - Job ID of submitted flight
   */
  public String cloneGcsBucket(
      UUID sourceWorkspaceId,
      UUID sourceResourceId,
      UUID destinationWorkspaceId,
      ApiJobControl jobControl,
      AuthenticatedUserRequest userRequest,
      @Nullable String destinationResourceName,
      @Nullable String destinationDescription,
      @Nullable String destinationBucketName,
      @Nullable String destinationLocation,
      @Nullable ApiCloningInstructionsEnum cloningInstructionsOverride) {
    stageService.assertMcWorkspace(destinationWorkspaceId, "cloneGcsBucket");

    final ControlledResource sourceBucketResource =
        controlledResourceService.getControlledResource(
            sourceWorkspaceId, sourceResourceId, userRequest);

    // Verify user can read source resource in Sam
    controlledResourceMetadataManager.validateControlledResourceAndAction(
        userRequest,
        sourceBucketResource.getWorkspaceId(),
        sourceBucketResource.getResourceId(),
        SamConstants.SamControlledResourceActions.READ_ACTION);

    // Write access to the target workspace will be established in the create flight
    final String jobDescription =
        String.format(
            "Clone controlled resource %s; id %s; name %s",
            sourceBucketResource.getResourceType(),
            sourceBucketResource.getResourceId(),
            sourceBucketResource.getName());

    final JobBuilder jobBuilder =
        jobService
            .newJob(
                jobDescription,
                jobControl.getId(),
                CloneControlledGcsBucketResourceFlight.class,
                sourceBucketResource,
                userRequest)
            .addParameter(
                WorkspaceFlightMapKeys.ControlledResourceKeys.DESTINATION_WORKSPACE_ID,
                destinationWorkspaceId)
            .addParameter(
                WorkspaceFlightMapKeys.ControlledResourceKeys.RESOURCE_NAME,
                destinationResourceName)
            .addParameter(
                WorkspaceFlightMapKeys.ControlledResourceKeys.RESOURCE_DESCRIPTION,
                destinationDescription)
            .addParameter(
                WorkspaceFlightMapKeys.ControlledResourceKeys.DESTINATION_BUCKET_NAME,
                destinationBucketName)
            .addParameter(
                WorkspaceFlightMapKeys.ControlledResourceKeys.LOCATION, destinationLocation)
            .addParameter(
                WorkspaceFlightMapKeys.ControlledResourceKeys.CLONING_INSTRUCTIONS,
                Optional.ofNullable(cloningInstructionsOverride)
                    .map(CloningInstructions::fromApiModel)
                    .orElse(sourceBucketResource.getCloningInstructions()));
    return jobBuilder.submit();
  }

  /** Starts a create controlled BigQuery dataset resource, blocking until its job is finished. */
  public ControlledBigQueryDatasetResource createBigQueryDataset(
      ControlledBigQueryDatasetResource resource,
      ApiGcpBigQueryDatasetCreationParameters creationParameters,
      List<ControlledResourceIamRole> privateResourceIamRoles,
      AuthenticatedUserRequest userRequest) {
    JobBuilder jobBuilder =
        controlledResourceService
            .commonCreationJobBuilder(
                resource,
                privateResourceIamRoles,
                new ApiJobControl().id(UUID.randomUUID().toString()),
                null,
                userRequest)
            .addParameter(
                WorkspaceFlightMapKeys.ControlledResourceKeys.CREATION_PARAMETERS,
                creationParameters);
    return jobBuilder.submitAndWait(ControlledBigQueryDatasetResource.class);
  }

  /** Starts an update controlled BigQuery dataset resource, blocking until its job is finished. */
  public ControlledBigQueryDatasetResource updateBqDataset(
      ControlledBigQueryDatasetResource resource,
      @Nullable ApiGcpBigQueryDatasetUpdateParameters updateParameters,
      AuthenticatedUserRequest userRequest,
      @Nullable String resourceName,
      @Nullable String resourceDescription) {
    final String jobDescription =
        String.format(
            "Update controlled BigQuery Dataset name %s ; resource id %s; resource name %s",
            resource.getDatasetName(), resource.getResourceId(), resource.getName());
    final JobBuilder jobBuilder =
        jobService
            .newJob(
                jobDescription,
                UUID.randomUUID().toString(), // no need to track ID
                UpdateControlledBigQueryDatasetResourceFlight.class,
                resource,
                userRequest)
            .addParameter(
                WorkspaceFlightMapKeys.ControlledResourceKeys.UPDATE_PARAMETERS, updateParameters)
            .addParameter(WorkspaceFlightMapKeys.ControlledResourceKeys.RESOURCE_NAME, resourceName)
            .addParameter(
                WorkspaceFlightMapKeys.ControlledResourceKeys.RESOURCE_DESCRIPTION,
                resourceDescription);
    return jobBuilder.submitAndWait(ControlledBigQueryDatasetResource.class);
  }

  /**
   * Make a clone of a BigQuery dataset
   *
   * @param sourceWorkspaceId - workspace ID of original dataset
   * @param sourceResourceId - resource ID of original dataset
   * @param destinationWorkspaceId - destination (sink) workspace ID
   * @param jobControl - job control structure (should already have ID)
   * @param userRequest - request object for this call
   * @param destinationResourceName - resource name. Uses source name if null
   * @param destinationDescription - description string for cloned dataset. Source description if
   *     null.
   * @param destinationDatasetName - name for new resource. Can equal source name. If null, a random
   *     name will be generated
   * @param destinationLocation - location override. Uses source location if null
   * @param cloningInstructionsOverride - Cloning instructions for this clone operation, overriding
   *     any existing instructions. Existing instructions are used if null.
   * @return
   */
  public String cloneBigQueryDataset(
      UUID sourceWorkspaceId,
      UUID sourceResourceId,
      UUID destinationWorkspaceId,
      ApiJobControl jobControl,
      AuthenticatedUserRequest userRequest,
      @Nullable String destinationResourceName,
      @Nullable String destinationDescription,
      @Nullable String destinationDatasetName,
      @Nullable String destinationLocation,
      @Nullable ApiCloningInstructionsEnum cloningInstructionsOverride) {
    stageService.assertMcWorkspace(destinationWorkspaceId, "cloneGcpBigQueryDataset");
    final ControlledResource sourceDatasetResource =
        controlledResourceService.getControlledResource(
            sourceWorkspaceId, sourceResourceId, userRequest);

    // Verify user can read source resource in Sam
    controlledResourceMetadataManager.validateControlledResourceAndAction(
        userRequest,
        sourceDatasetResource.getWorkspaceId(),
        sourceDatasetResource.getResourceId(),
        SamConstants.SamControlledResourceActions.READ_ACTION);

    // Write access to the target workspace will be established in the create flight
    final String jobDescription =
        String.format(
            "Clone controlled resource %s; id %s; name %s",
            sourceDatasetResource.getResourceType(),
            sourceDatasetResource.getResourceId(),
            sourceDatasetResource.getName());
    final JobBuilder jobBuilder =
        jobService
            .newJob(
                jobDescription,
                jobControl.getId(),
                CloneControlledGcpBigQueryDatasetResourceFlight.class,
                sourceDatasetResource,
                userRequest)
            .addParameter(
                WorkspaceFlightMapKeys.ControlledResourceKeys.DESTINATION_WORKSPACE_ID,
                destinationWorkspaceId)
            .addParameter(
                WorkspaceFlightMapKeys.ControlledResourceKeys.RESOURCE_NAME,
                destinationResourceName)
            .addParameter(
                WorkspaceFlightMapKeys.ControlledResourceKeys.RESOURCE_DESCRIPTION,
                destinationDescription)
            .addParameter(
                WorkspaceFlightMapKeys.ControlledResourceKeys.LOCATION, destinationLocation)
            .addParameter(
                WorkspaceFlightMapKeys.ControlledResourceKeys.DESTINATION_DATASET_NAME,
                destinationDatasetName)
            .addParameter(
                WorkspaceFlightMapKeys.ControlledResourceKeys.CLONING_INSTRUCTIONS,
                // compute effective cloning instructions
                Optional.ofNullable(cloningInstructionsOverride)
                    .map(CloningInstructions::fromApiModel)
                    .orElse(sourceDatasetResource.getCloningInstructions()));
    return jobBuilder.submit();
  }

  /** Starts a create controlled AI Notebook instance resource job, returning the job id. */
  public String createAiNotebookInstance(
      ControlledAiNotebookInstanceResource resource,
      ApiGcpAiNotebookInstanceCreationParameters creationParameters,
      List<ControlledResourceIamRole> privateResourceIamRoles,
      ApiJobControl jobControl,
      String resultPath,
      AuthenticatedUserRequest userRequest) {
    if (privateResourceIamRoles.stream()
        .noneMatch(role -> role.equals(ControlledResourceIamRole.WRITER))) {
      throw new BadRequestException(
          "A private, controlled AI Notebook instance must have the writer role or else it is not useful.");
    }
    JobBuilder jobBuilder =
        controlledResourceService.commonCreationJobBuilder(
            resource, privateResourceIamRoles, jobControl, resultPath, userRequest);
    jobBuilder.addParameter(
        WorkspaceFlightMapKeys.ControlledResourceKeys.CREATE_NOTEBOOK_PARAMETERS,
        creationParameters);
    return jobBuilder.submit();
  }
}
