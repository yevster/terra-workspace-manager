package bio.terra.workspace.app.controller;

import bio.terra.workspace.common.utils.ControllerUtils;
import bio.terra.workspace.common.utils.ControllerValidationUtils;
import bio.terra.workspace.generated.controller.WorkspaceApi;
import bio.terra.workspace.generated.model.ApiCloneWorkspaceRequest;
import bio.terra.workspace.generated.model.ApiCloneWorkspaceResult;
import bio.terra.workspace.generated.model.ApiClonedWorkspace;
import bio.terra.workspace.generated.model.ApiCloudPlatform;
import bio.terra.workspace.generated.model.ApiCreateCloudContextRequest;
import bio.terra.workspace.generated.model.ApiCreateCloudContextResult;
import bio.terra.workspace.generated.model.ApiCreateDataReferenceRequestBody;
import bio.terra.workspace.generated.model.ApiCreateWorkspaceRequestBody;
import bio.terra.workspace.generated.model.ApiCreatedWorkspace;
import bio.terra.workspace.generated.model.ApiDataReferenceDescription;
import bio.terra.workspace.generated.model.ApiDataReferenceList;
import bio.terra.workspace.generated.model.ApiDataRepoSnapshot;
import bio.terra.workspace.generated.model.ApiGcpContext;
import bio.terra.workspace.generated.model.ApiGrantRoleRequestBody;
import bio.terra.workspace.generated.model.ApiIamRole;
import bio.terra.workspace.generated.model.ApiJobReport.StatusEnum;
import bio.terra.workspace.generated.model.ApiReferenceTypeEnum;
import bio.terra.workspace.generated.model.ApiRoleBinding;
import bio.terra.workspace.generated.model.ApiRoleBindingList;
import bio.terra.workspace.generated.model.ApiUpdateDataReferenceRequestBody;
import bio.terra.workspace.generated.model.ApiUpdateWorkspaceRequestBody;
import bio.terra.workspace.generated.model.ApiWorkspaceDescription;
import bio.terra.workspace.generated.model.ApiWorkspaceDescriptionList;
import bio.terra.workspace.generated.model.ApiWorkspaceStageModel;
import bio.terra.workspace.service.iam.AuthenticatedUserRequest;
import bio.terra.workspace.service.iam.AuthenticatedUserRequestFactory;
import bio.terra.workspace.service.iam.SamRethrow;
import bio.terra.workspace.service.iam.SamService;
import bio.terra.workspace.service.iam.exception.InvalidRoleException;
import bio.terra.workspace.service.iam.model.WsmIamRole;
import bio.terra.workspace.service.job.JobService;
import bio.terra.workspace.service.job.JobService.AsyncJobResult;
import bio.terra.workspace.service.resource.ValidationUtils;
import bio.terra.workspace.service.resource.WsmResourceType;
import bio.terra.workspace.service.resource.model.CloningInstructions;
import bio.terra.workspace.service.resource.referenced.ReferencedDataRepoSnapshotResource;
import bio.terra.workspace.service.resource.referenced.ReferencedResource;
import bio.terra.workspace.service.resource.referenced.ReferencedResourceService;
import bio.terra.workspace.service.resource.referenced.exception.InvalidReferenceException;
import bio.terra.workspace.service.spendprofile.SpendProfileId;
import bio.terra.workspace.service.workspace.WorkspaceService;
import bio.terra.workspace.service.workspace.model.GcpCloudContext;
import bio.terra.workspace.service.workspace.model.Workspace;
import bio.terra.workspace.service.workspace.model.WorkspaceRequest;
import bio.terra.workspace.service.workspace.model.WorkspaceStage;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;

@Controller
public class WorkspaceApiController implements WorkspaceApi {
  private final WorkspaceService workspaceService;
  private final JobService jobService;
  private final SamService samService;
  private final AuthenticatedUserRequestFactory authenticatedUserRequestFactory;
  private final HttpServletRequest request;
  private final ReferencedResourceService referenceResourceService;

  @Autowired
  public WorkspaceApiController(
      WorkspaceService workspaceService,
      JobService jobService,
      SamService samService,
      AuthenticatedUserRequestFactory authenticatedUserRequestFactory,
      HttpServletRequest request,
      ReferencedResourceService referenceResourceService) {
    this.workspaceService = workspaceService;
    this.jobService = jobService;
    this.samService = samService;
    this.authenticatedUserRequestFactory = authenticatedUserRequestFactory;
    this.request = request;
    this.referenceResourceService = referenceResourceService;
  }

  private final Logger logger = LoggerFactory.getLogger(WorkspaceApiController.class);

  private AuthenticatedUserRequest getAuthenticatedInfo() {
    return authenticatedUserRequestFactory.from(request);
  }

  @Override
  public ResponseEntity<ApiCreatedWorkspace> createWorkspace(
      @RequestBody ApiCreateWorkspaceRequestBody body) {
    AuthenticatedUserRequest userRequest = getAuthenticatedInfo();
    logger.info("Creating workspace {} for {}", body.getId(), userRequest.getEmail());

    // Existing client libraries should not need to know about the stage, as they won't use any of
    // the features it gates. If stage isn't specified in a create request, we default to
    // RAWLS_WORKSPACE.
    ApiWorkspaceStageModel requestStage = body.getStage();
    requestStage = (requestStage == null ? ApiWorkspaceStageModel.RAWLS_WORKSPACE : requestStage);
    WorkspaceStage internalStage = WorkspaceStage.fromApiModel(requestStage);
    Optional<SpendProfileId> spendProfileId =
        Optional.ofNullable(body.getSpendProfile()).map(SpendProfileId::create);

    WorkspaceRequest internalRequest =
        WorkspaceRequest.builder()
            .workspaceId(body.getId())
            .spendProfileId(spendProfileId)
            .workspaceStage(internalStage)
            .displayName(Optional.ofNullable(body.getDisplayName()))
            .description(Optional.ofNullable(body.getDescription()))
            .build();
    UUID createdId = workspaceService.createWorkspace(internalRequest, userRequest);

    ApiCreatedWorkspace responseWorkspace = new ApiCreatedWorkspace().id(createdId);
    logger.info("Created workspace {} for {}", responseWorkspace, userRequest.getEmail());

    return new ResponseEntity<>(responseWorkspace, HttpStatus.OK);
  }

  @Override
  public ResponseEntity<ApiWorkspaceDescriptionList> listWorkspaces(Integer offset, Integer limit) {
    AuthenticatedUserRequest userRequest = getAuthenticatedInfo();
    logger.info("Listing workspaces for {}", userRequest.getEmail());
    List<Workspace> workspaces = workspaceService.listWorkspaces(userRequest, offset, limit);
    var response =
        new ApiWorkspaceDescriptionList()
            .workspaces(
                workspaces.stream()
                    .map(this::buildWorkspaceDescription)
                    .collect(Collectors.toList()));
    return new ResponseEntity<>(response, HttpStatus.OK);
  }

  private ApiWorkspaceDescription buildWorkspaceDescription(Workspace workspace) {
    ApiGcpContext gcpContext =
        workspace.getGcpCloudContext().map(GcpCloudContext::toApi).orElse(null);
    // Note projectId will be null here if no GCP cloud context exists.
    // When we have another cloud context, we will need to do a similar retrieval for it.
    return new ApiWorkspaceDescription()
        .id(workspace.getWorkspaceId())
        .spendProfile(workspace.getSpendProfileId().map(SpendProfileId::id).orElse(null))
        .stage(workspace.getWorkspaceStage().toApiModel())
        .gcpContext(gcpContext)
        .displayName(workspace.getDisplayName().orElse(null))
        .description(workspace.getDescription().orElse(null));
  }

  @Override
  public ResponseEntity<ApiWorkspaceDescription> getWorkspace(
      @PathVariable("workspaceId") UUID id) {
    AuthenticatedUserRequest userRequest = getAuthenticatedInfo();
    logger.info("Getting workspace {} for {}", id, userRequest.getEmail());
    Workspace workspace = workspaceService.getWorkspace(id, userRequest);
    ApiWorkspaceDescription desc = buildWorkspaceDescription(workspace);
    logger.info("Got workspace {} for {}", desc, userRequest.getEmail());

    return new ResponseEntity<>(desc, HttpStatus.OK);
  }

  @Override
  public ResponseEntity<ApiWorkspaceDescription> updateWorkspace(
      @PathVariable("workspaceId") UUID workspaceId,
      @RequestBody ApiUpdateWorkspaceRequestBody body) {
    AuthenticatedUserRequest userRequest = getAuthenticatedInfo();
    logger.info("Updating workspace {} for {}", workspaceId, userRequest.getEmail());
    Workspace workspace =
        workspaceService.updateWorkspace(
            userRequest, workspaceId, body.getDisplayName(), body.getDescription());

    ApiWorkspaceDescription desc = buildWorkspaceDescription(workspace);
    logger.info("Updated workspace {} for {}", desc, userRequest.getEmail());

    return new ResponseEntity<>(desc, HttpStatus.OK);
  }

  @Override
  public ResponseEntity<Void> deleteWorkspace(@PathVariable("workspaceId") UUID id) {
    AuthenticatedUserRequest userRequest = getAuthenticatedInfo();
    logger.info("Deleting workspace {} for {}", id, userRequest.getEmail());
    workspaceService.deleteWorkspace(id, userRequest);
    logger.info("Deleted workspace {} for {}", id, userRequest.getEmail());

    return new ResponseEntity<>(HttpStatus.NO_CONTENT);
  }

  // TODO(PF-404): the following DataReference endpoints are deprecated and will go away

  @Override
  public ResponseEntity<ApiDataReferenceDescription> createDataReference(
      @PathVariable("workspaceId") UUID id, @RequestBody ApiCreateDataReferenceRequestBody body) {
    AuthenticatedUserRequest userRequest = getAuthenticatedInfo();
    logger.info(
        "Creating data reference in workspace {} for {} with body {}",
        id,
        userRequest.getEmail(),
        body);

    ControllerValidationUtils.validate(body);
    ValidationUtils.validateResourceName(body.getName());

    var resource =
        new ReferencedDataRepoSnapshotResource(
            id,
            UUID.randomUUID(), // mint a resource id for this bucket
            body.getName(),
            body.getDescription(),
            CloningInstructions.fromApiModel(body.getCloningInstructions()),
            body.getReference().getInstanceName(),
            body.getReference().getSnapshot());

    ReferencedResource referenceResource =
        referenceResourceService.createReferenceResource(resource, getAuthenticatedInfo());
    ApiDataReferenceDescription response = makeApiDataReferenceDescription(referenceResource);
    return new ResponseEntity<>(response, HttpStatus.OK);
  }

  @Override
  public ResponseEntity<ApiDataReferenceDescription> getDataReference(
      @PathVariable("workspaceId") UUID workspaceId,
      @PathVariable("referenceId") UUID referenceId) {
    AuthenticatedUserRequest userRequest = getAuthenticatedInfo();
    logger.info(
        "Getting data reference by id {} in workspace {} for {}",
        referenceId,
        workspaceId,
        userRequest.getEmail());

    ReferencedResource referenceResource =
        referenceResourceService.getReferenceResource(workspaceId, referenceId, userRequest);

    // TODO(PF-404): this endpoint's return type does not support reference types beyond snapshots.
    // Clients should migrate to type-specific endpoints, and this endpoint should be removed.
    if (referenceResource.getResourceType() != WsmResourceType.DATA_REPO_SNAPSHOT) {
      throw new InvalidReferenceException(
          "This endpoint does not support non-snapshot references. Use the newer type-specific endpoints instead.");
    }

    ApiDataReferenceDescription response = makeApiDataReferenceDescription(referenceResource);
    return new ResponseEntity<>(response, HttpStatus.OK);
  }

  @Override
  public ResponseEntity<ApiDataReferenceDescription> getDataReferenceByName(
      @PathVariable("workspaceId") UUID workspaceId,
      @PathVariable("referenceType") ApiReferenceTypeEnum referenceType,
      @PathVariable("name") String name) {
    AuthenticatedUserRequest userRequest = getAuthenticatedInfo();
    // TODO(PF-404): this endpoint's return type does not support reference types beyond snapshots.
    // Clients should migrate to type-specific endpoints, and this endpoint should be removed.
    if (referenceType != ApiReferenceTypeEnum.DATA_REPO_SNAPSHOT) {
      throw new InvalidReferenceException(
          "This endpoint does not support non-snapshot references. Use the newer type-specific endpoints instead.");
    }
    ValidationUtils.validateResourceName(name);

    ReferencedResource referenceResource =
        referenceResourceService.getReferenceResourceByName(workspaceId, name, userRequest);
    ApiDataReferenceDescription response = makeApiDataReferenceDescription(referenceResource);
    return new ResponseEntity<>(response, HttpStatus.OK);
  }

  @Override
  public ResponseEntity<Void> updateDataReference(
      @PathVariable("workspaceId") UUID id,
      @PathVariable("referenceId") UUID referenceId,
      @RequestBody ApiUpdateDataReferenceRequestBody body) {
    AuthenticatedUserRequest userRequest = getAuthenticatedInfo();

    if (body.getName() == null && body.getDescription() == null) {
      throw new InvalidReferenceException("Must specify name or description to update.");
    }

    if (body.getName() != null) {
      ValidationUtils.validateResourceName(body.getName());
    }

    referenceResourceService.updateReferenceResource(
        id, referenceId, body.getName(), body.getDescription(), userRequest);
    return new ResponseEntity<>(HttpStatus.NO_CONTENT);
  }

  @Override
  public ResponseEntity<Void> deleteDataReference(
      @PathVariable("workspaceId") UUID workspaceId,
      @PathVariable("referenceId") UUID referenceId) {
    AuthenticatedUserRequest userRequest = getAuthenticatedInfo();
    logger.info(
        "Deleting data reference by id {} in workspace {} for {}",
        referenceId,
        workspaceId,
        userRequest.getEmail());

    referenceResourceService.deleteReferenceResource(workspaceId, referenceId, userRequest);

    logger.info(
        "Deleted data reference by id {} in workspace {} for {}",
        referenceId,
        workspaceId,
        userRequest.getEmail());

    return new ResponseEntity<>(HttpStatus.NO_CONTENT);
  }

  @Override
  public ResponseEntity<ApiDataReferenceList> enumerateReferences(
      @PathVariable("workspaceId") UUID id,
      @Valid @RequestParam(value = "offset", required = false, defaultValue = "0") Integer offset,
      @Valid @RequestParam(value = "limit", required = false, defaultValue = "10") Integer limit) {
    AuthenticatedUserRequest userRequest = getAuthenticatedInfo();
    logger.info(
        "Getting snapshot data references in workspace {} for {}", id, userRequest.getEmail());
    ControllerValidationUtils.validatePaginationParams(offset, limit);
    List<ReferencedResource> enumerateResult =
        referenceResourceService.enumerateReferences(id, offset, limit, userRequest);
    // TODO(PF-404): this is a workaround until clients migrate off this endpoint.
    ApiDataReferenceList responseList = new ApiDataReferenceList();
    for (ReferencedResource resource : enumerateResult) {
      if (resource.getResourceType() == WsmResourceType.DATA_REPO_SNAPSHOT) {
        responseList.addResourcesItem(makeApiDataReferenceDescription(resource));
      }
    }
    return ResponseEntity.ok(responseList);
  }

  private ApiDataReferenceDescription makeApiDataReferenceDescription(
      ReferencedResource referenceResource) {
    ReferencedDataRepoSnapshotResource snapshotResource =
        referenceResource.castToDataRepoSnapshotResource();
    var reference =
        new ApiDataRepoSnapshot()
            .instanceName(snapshotResource.getInstanceName())
            .snapshot(snapshotResource.getSnapshotId());
    return new ApiDataReferenceDescription()
        .referenceId(referenceResource.getResourceId())
        .name(referenceResource.getName())
        .description(referenceResource.getDescription())
        .workspaceId(referenceResource.getWorkspaceId())
        .cloningInstructions(referenceResource.getCloningInstructions().toApiModel())
        .referenceType(ApiReferenceTypeEnum.DATA_REPO_SNAPSHOT)
        .reference(reference);
  }

  @Override
  public ResponseEntity<Void> grantRole(
      @PathVariable("workspaceId") UUID id,
      @PathVariable("role") ApiIamRole role,
      @RequestBody ApiGrantRoleRequestBody body) {
    ControllerValidationUtils.validateEmail(body.getMemberEmail());
    if (role == ApiIamRole.APPLICATION) {
      throw new InvalidRoleException(
          "Users cannot grant role APPLICATION. Use application registration instead.");
    }
    SamRethrow.onInterrupted(
        () ->
            samService.grantWorkspaceRole(
                id, getAuthenticatedInfo(), WsmIamRole.fromApiModel(role), body.getMemberEmail()),
        "grantWorkspaceRole");
    return new ResponseEntity<>(HttpStatus.NO_CONTENT);
  }

  @Override
  public ResponseEntity<Void> removeRole(
      @PathVariable("workspaceId") UUID id,
      @PathVariable("role") ApiIamRole role,
      @PathVariable("memberEmail") String memberEmail) {
    ControllerValidationUtils.validateEmail(memberEmail);
    if (role == ApiIamRole.APPLICATION) {
      throw new InvalidRoleException(
          "Users cannot remove role APPLICATION. Use application registration instead.");
    }
    AuthenticatedUserRequest userRequest = getAuthenticatedInfo();
    workspaceService.removeWorkspaceRoleFromUser(
        id, WsmIamRole.fromApiModel(role), memberEmail, userRequest);
    return new ResponseEntity<>(HttpStatus.NO_CONTENT);
  }

  @Override
  public ResponseEntity<ApiRoleBindingList> getRoles(@PathVariable("workspaceId") UUID id) {
    List<bio.terra.workspace.service.iam.model.RoleBinding> bindingList =
        SamRethrow.onInterrupted(
            () -> samService.listRoleBindings(id, getAuthenticatedInfo()), "listRoleBindings");
    ApiRoleBindingList responseList = new ApiRoleBindingList();
    for (bio.terra.workspace.service.iam.model.RoleBinding roleBinding : bindingList) {
      responseList.add(
          new ApiRoleBinding().role(roleBinding.role().toApiModel()).members(roleBinding.users()));
    }
    return new ResponseEntity<>(responseList, HttpStatus.OK);
  }

  @Override
  public ResponseEntity<ApiCreateCloudContextResult> createCloudContext(
      UUID id, @Valid ApiCreateCloudContextRequest body) {
    ControllerValidationUtils.validateCloudPlatform(body.getCloudPlatform());
    AuthenticatedUserRequest userRequest = getAuthenticatedInfo();
    String jobId = body.getJobControl().getId();
    String resultPath = ControllerUtils.getAsyncResultEndpoint(request, jobId);

    // For now, the cloud type is always GCP and that is guaranteed in the validate.
    workspaceService.createGcpCloudContext(id, jobId, userRequest, resultPath);
    ApiCreateCloudContextResult response = fetchCreateCloudContextResult(jobId, userRequest);
    return new ResponseEntity<>(
        response, ControllerUtils.getAsyncResponseCode(response.getJobReport()));
  }

  @Override
  public ResponseEntity<ApiCreateCloudContextResult> getCreateCloudContextResult(
      UUID id, String jobId) {
    AuthenticatedUserRequest userRequest = getAuthenticatedInfo();
    ApiCreateCloudContextResult response = fetchCreateCloudContextResult(jobId, userRequest);
    return new ResponseEntity<>(
        response, ControllerUtils.getAsyncResponseCode(response.getJobReport()));
  }

  private ApiCreateCloudContextResult fetchCreateCloudContextResult(
      String jobId, AuthenticatedUserRequest userRequest) {
    final AsyncJobResult<GcpCloudContext> jobResult =
        jobService.retrieveAsyncJobResult(jobId, GcpCloudContext.class, userRequest);

    final ApiGcpContext gcpContext;
    if (jobResult.getJobReport().getStatus().equals(StatusEnum.SUCCEEDED)) {
      gcpContext = new ApiGcpContext().projectId(jobResult.getResult().getGcpProjectId());
    } else {
      gcpContext = null;
    }
    return new ApiCreateCloudContextResult()
        .jobReport(jobResult.getJobReport())
        .errorReport(jobResult.getApiErrorReport())
        .gcpContext(gcpContext);
  }

  @Override
  public ResponseEntity<Void> deleteCloudContext(UUID id, ApiCloudPlatform cloudPlatform) {
    AuthenticatedUserRequest userRequest = getAuthenticatedInfo();
    ControllerValidationUtils.validateCloudPlatform(cloudPlatform);
    workspaceService.deleteGcpCloudContext(id, userRequest);
    return new ResponseEntity<>(HttpStatus.NO_CONTENT);
  }

  @Override
  public ResponseEntity<String> enablePet(UUID workspaceId) {
    AuthenticatedUserRequest userRequest = getAuthenticatedInfo();
    String petSaEmail =
        workspaceService.enablePetServiceAccountImpersonation(workspaceId, userRequest);
    return new ResponseEntity<>(petSaEmail, HttpStatus.OK);
  }

  /**
   * Clone an entire workspace by creating a new workspace and cloning the workspace's resources
   * into it.
   *
   * @param workspaceId - ID of source workspace
   * @param body - request body
   * @return - result structure for the overall clone operation with details for each resource
   */
  @Override
  public ResponseEntity<ApiCloneWorkspaceResult> cloneWorkspace(
      UUID workspaceId, @Valid ApiCloneWorkspaceRequest body) {
    final String jobId =
        workspaceService.cloneWorkspace(
            workspaceId,
            getAuthenticatedInfo(),
            body.getSpendProfile(),
            body.getLocation(),
            body.getDisplayName(),
            body.getDescription());
    final ApiCloneWorkspaceResult result = fetchCloneWorkspaceResult(jobId, getAuthenticatedInfo());
    return new ResponseEntity<>(
        result, ControllerUtils.getAsyncResponseCode(result.getJobReport()));
  }
  /**
   * Return the workspace clone result, including job result and error result.
   *
   * @param workspaceId - source workspace ID
   * @param jobId - ID of flight
   * @return - response with result
   */
  @Override
  public ResponseEntity<ApiCloneWorkspaceResult> getCloneWorkspaceResult(
      UUID workspaceId, String jobId) {
    final AuthenticatedUserRequest userRequest = getAuthenticatedInfo();
    final ApiCloneWorkspaceResult result = fetchCloneWorkspaceResult(jobId, userRequest);
    return new ResponseEntity<>(
        result, ControllerUtils.getAsyncResponseCode(result.getJobReport()));
  }

  // Retrieve the async result or progress for clone workspace.
  private ApiCloneWorkspaceResult fetchCloneWorkspaceResult(
      String jobId, AuthenticatedUserRequest userRequest) {
    final AsyncJobResult<ApiClonedWorkspace> jobResult =
        jobService.retrieveAsyncJobResult(jobId, ApiClonedWorkspace.class, userRequest);
    return new ApiCloneWorkspaceResult()
        .jobReport(jobResult.getJobReport())
        .errorReport(jobResult.getApiErrorReport())
        .workspace(jobResult.getResult());
  }
}
