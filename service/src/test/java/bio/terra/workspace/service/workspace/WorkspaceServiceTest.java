package bio.terra.workspace.service.workspace;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;

import bio.terra.common.exception.ErrorReportException;
import bio.terra.common.exception.MissingRequiredFieldException;
import bio.terra.common.exception.UnauthorizedException;
import bio.terra.common.sam.exception.SamExceptionFactory;
import bio.terra.common.sam.exception.SamInternalServerErrorException;
import bio.terra.stairway.FlightDebugInfo;
import bio.terra.stairway.StepStatus;
import bio.terra.workspace.common.BaseConnectedTest;
import bio.terra.workspace.db.ResourceDao;
import bio.terra.workspace.db.exception.WorkspaceNotFoundException;
import bio.terra.workspace.service.crl.CrlService;
import bio.terra.workspace.service.datarepo.DataRepoService;
import bio.terra.workspace.service.iam.AuthenticatedUserRequest;
import bio.terra.workspace.service.iam.SamService;
import bio.terra.workspace.service.iam.model.SamConstants;
import bio.terra.workspace.service.job.JobService;
import bio.terra.workspace.service.job.exception.InvalidResultStateException;
import bio.terra.workspace.service.resource.exception.ResourceNotFoundException;
import bio.terra.workspace.service.resource.model.CloningInstructions;
import bio.terra.workspace.service.resource.referenced.ReferencedDataRepoSnapshotResource;
import bio.terra.workspace.service.resource.referenced.ReferencedResourceService;
import bio.terra.workspace.service.spendprofile.SpendConnectedTestUtils;
import bio.terra.workspace.service.spendprofile.SpendProfileId;
import bio.terra.workspace.service.spendprofile.exceptions.SpendUnauthorizedException;
import bio.terra.workspace.service.workspace.exceptions.DuplicateWorkspaceException;
import bio.terra.workspace.service.workspace.exceptions.MissingSpendProfileException;
import bio.terra.workspace.service.workspace.exceptions.NoBillingAccountException;
import bio.terra.workspace.service.workspace.exceptions.StageDisabledException;
import bio.terra.workspace.service.workspace.flight.DeleteProjectStep;
import bio.terra.workspace.service.workspace.flight.DeleteWorkspaceAuthzStep;
import bio.terra.workspace.service.workspace.flight.DeleteWorkspaceStateStep;
import bio.terra.workspace.service.workspace.model.GcpCloudContext;
import bio.terra.workspace.service.workspace.model.Workspace;
import bio.terra.workspace.service.workspace.model.WorkspaceRequest;
import bio.terra.workspace.service.workspace.model.WorkspaceStage;
import com.google.api.services.cloudresourcemanager.v3.model.Project;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.broadinstitute.dsde.workbench.client.sam.ApiException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;

class WorkspaceServiceTest extends BaseConnectedTest {
  /** A fake authenticated user request. */
  private static final AuthenticatedUserRequest USER_REQUEST =
      new AuthenticatedUserRequest()
          .token(Optional.of("fake-token"))
          .email("fake@email.com")
          .subjectId("fakeID123");

  @Autowired private WorkspaceService workspaceService;
  @Autowired private JobService jobService;
  @Autowired private CrlService crl;
  @Autowired private SpendConnectedTestUtils spendUtils;
  @Autowired private ReferencedResourceService referenceResourceService;
  @Autowired private ResourceDao resourceDao;
  @MockBean private DataRepoService dataRepoService;
  /** Mock SamService does nothing for all calls that would throw if unauthorized. */
  @MockBean private SamService mockSamService;

  @BeforeEach
  void setup() throws Exception {
    doReturn(true).when(dataRepoService).snapshotReadable(any(), any(), any());
    // By default, allow all spend link calls as authorized. (All other isAuthorized calls return
    // false by Mockito default.
    Mockito.when(
            mockSamService.isAuthorized(
                Mockito.any(),
                Mockito.eq(SamConstants.SPEND_PROFILE_RESOURCE),
                Mockito.any(),
                Mockito.eq(SamConstants.SPEND_PROFILE_LINK_ACTION)))
        .thenReturn(true);
    // Return a valid google group for cloud sync, as Google validates groups added to GCP projects.
    Mockito.when(mockSamService.syncWorkspacePolicy(any(), any(), any()))
        .thenReturn("terra-workspace-manager-test-group@googlegroups.com");
  }

  /**
   * Reset the {@link FlightDebugInfo} on the {@link JobService} to not interfere with other tests.
   */
  @AfterEach
  public void resetFlightDebugInfo() {
    jobService.setFlightDebugInfoForTest(null);
  }

  @Test
  void testGetMissingWorkspace() {
    assertThrows(
        WorkspaceNotFoundException.class,
        () -> workspaceService.getWorkspace(UUID.randomUUID(), USER_REQUEST));
  }

  @Test
  void testGetExistingWorkspace() {
    WorkspaceRequest request = defaultRequestBuilder(UUID.randomUUID()).build();
    workspaceService.createWorkspace(request, USER_REQUEST);

    assertEquals(
        request.workspaceId(),
        workspaceService.getWorkspace(request.workspaceId(), USER_REQUEST).getWorkspaceId());
  }

  @Test
  void testGetForbiddenMissingWorkspace() throws Exception {
    doThrow(new UnauthorizedException("forbid!"))
        .when(mockSamService)
        .checkAuthz(any(), any(), any(), any());
    assertThrows(
        WorkspaceNotFoundException.class,
        () -> workspaceService.getWorkspace(UUID.randomUUID(), USER_REQUEST));
  }

  @Test
  void testGetForbiddenExistingWorkspace() throws Exception {
    WorkspaceRequest request = defaultRequestBuilder(UUID.randomUUID()).build();
    workspaceService.createWorkspace(request, USER_REQUEST);

    doThrow(new UnauthorizedException("forbid!"))
        .when(mockSamService)
        .checkAuthz(any(), any(), any(), any());
    assertThrows(
        UnauthorizedException.class,
        () -> workspaceService.getWorkspace(request.workspaceId(), USER_REQUEST));
  }

  @Test
  void testWorkspaceStagePersists() {
    WorkspaceRequest mcWorkspaceRequest =
        defaultRequestBuilder(UUID.randomUUID())
            .workspaceStage(WorkspaceStage.MC_WORKSPACE)
            .build();
    workspaceService.createWorkspace(mcWorkspaceRequest, USER_REQUEST);
    Workspace createdWorkspace =
        workspaceService.getWorkspace(mcWorkspaceRequest.workspaceId(), USER_REQUEST);
    assertEquals(mcWorkspaceRequest.workspaceId(), createdWorkspace.getWorkspaceId());
    assertEquals(WorkspaceStage.MC_WORKSPACE, createdWorkspace.getWorkspaceStage());
  }

  @Test
  void duplicateWorkspaceIdRequestsRejected() {
    WorkspaceRequest request = defaultRequestBuilder(UUID.randomUUID()).build();
    workspaceService.createWorkspace(request, USER_REQUEST);
    WorkspaceRequest duplicateWorkspace = defaultRequestBuilder(request.workspaceId()).build();
    assertThrows(
        DuplicateWorkspaceException.class,
        () -> workspaceService.createWorkspace(duplicateWorkspace, USER_REQUEST));
  }

  @Test
  void duplicateOperationSharesFailureResponse() throws Exception {
    String errorMsg = "fake SAM error message";
    doThrow(SamExceptionFactory.create(errorMsg, new ApiException(("test"))))
        .when(mockSamService)
        .createWorkspaceWithDefaults(any(), any());

    assertThrows(
        ErrorReportException.class,
        () ->
            workspaceService.createWorkspace(
                defaultRequestBuilder(UUID.randomUUID()).build(), USER_REQUEST));
    // This second call shares the above operation ID, and so should return the same exception
    // instead of a more generic internal Stairway exception.
    assertThrows(
        ErrorReportException.class,
        () ->
            workspaceService.createWorkspace(
                defaultRequestBuilder(UUID.randomUUID()).build(), USER_REQUEST));
  }

  @Test
  void testWithSpendProfile() {
    Optional<SpendProfileId> spendProfileId = Optional.of(SpendProfileId.create("foo"));
    WorkspaceRequest request =
        defaultRequestBuilder(UUID.randomUUID()).spendProfileId(spendProfileId).build();
    workspaceService.createWorkspace(request, USER_REQUEST);

    Workspace createdWorkspace = workspaceService.getWorkspace(request.workspaceId(), USER_REQUEST);
    assertEquals(request.workspaceId(), createdWorkspace.getWorkspaceId());
    assertEquals(spendProfileId, createdWorkspace.getSpendProfileId());
  }

  @Test
  void testWithDisplayNameAndDescription() {
    String name = "My workspace";
    String description = "The greatest workspace";
    WorkspaceRequest request =
        defaultRequestBuilder(UUID.randomUUID())
            .displayName(Optional.of(name))
            .description(Optional.of(description))
            .build();
    workspaceService.createWorkspace(request, USER_REQUEST);

    Workspace createdWorkspace = workspaceService.getWorkspace(request.workspaceId(), USER_REQUEST);
    assertEquals(request.workspaceId(), createdWorkspace.getWorkspaceId());
    assertEquals(name, createdWorkspace.getDisplayName().get());
    assertEquals(description, createdWorkspace.getDescription().get());
  }

  @Test
  void testUpdateWorkspace() {
    WorkspaceRequest request = defaultRequestBuilder(UUID.randomUUID()).build();
    workspaceService.createWorkspace(request, USER_REQUEST);
    Workspace createdWorkspace = workspaceService.getWorkspace(request.workspaceId(), USER_REQUEST);
    assertEquals(request.workspaceId(), createdWorkspace.getWorkspaceId());
    assertEquals("", createdWorkspace.getDisplayName().get());
    assertEquals("", createdWorkspace.getDescription().get());

    UUID workspaceId = request.workspaceId();
    String name = "My workspace";
    String description = "The greatest workspace";

    Workspace updatedWorkspace =
        workspaceService.updateWorkspace(USER_REQUEST, workspaceId, name, description);

    assertEquals(name, updatedWorkspace.getDisplayName().get());
    assertEquals(description, updatedWorkspace.getDescription().get());

    String otherDescription = "The deprecated workspace";

    Workspace secondUpdatedWorkspace =
        workspaceService.updateWorkspace(USER_REQUEST, workspaceId, null, otherDescription);

    // Since name is null, leave it alone. Description should be updated.
    assertEquals(name, secondUpdatedWorkspace.getDisplayName().get());
    assertEquals(otherDescription, secondUpdatedWorkspace.getDescription().get());

    // Sending through empty strings clears the values.
    Workspace thirdUpdatedWorkspace =
        workspaceService.updateWorkspace(USER_REQUEST, workspaceId, "", "");
    assertEquals("", thirdUpdatedWorkspace.getDisplayName().get());
    assertEquals("", thirdUpdatedWorkspace.getDescription().get());

    assertThrows(
        MissingRequiredFieldException.class,
        () -> workspaceService.updateWorkspace(USER_REQUEST, workspaceId, null, null));
  }

  @Test
  void testHandlesSamError() throws Exception {
    String apiErrorMsg = "test";
    ErrorReportException testex = new SamInternalServerErrorException(apiErrorMsg);
    doThrow(testex).when(mockSamService).createWorkspaceWithDefaults(any(), any());
    ErrorReportException exception =
        assertThrows(
            SamInternalServerErrorException.class,
            () ->
                workspaceService.createWorkspace(
                    defaultRequestBuilder(UUID.randomUUID()).build(), USER_REQUEST));
    assertEquals(apiErrorMsg, exception.getMessage());
  }

  @Test
  void createAndDeleteWorkspace() {
    WorkspaceRequest request = defaultRequestBuilder(UUID.randomUUID()).build();
    workspaceService.createWorkspace(request, USER_REQUEST);

    workspaceService.deleteWorkspace(request.workspaceId(), USER_REQUEST);
    assertThrows(
        WorkspaceNotFoundException.class,
        () -> workspaceService.getWorkspace(request.workspaceId(), USER_REQUEST));
  }

  @Test
  void deleteMcWorkspaceDoSteps() {
    WorkspaceRequest request =
        defaultRequestBuilder(UUID.randomUUID())
            .workspaceStage(WorkspaceStage.MC_WORKSPACE)
            .build();
    workspaceService.createWorkspace(request, USER_REQUEST);

    Map<String, StepStatus> doFailures = new HashMap<>();
    doFailures.put(DeleteProjectStep.class.getName(), StepStatus.STEP_RESULT_FAILURE_RETRY);
    doFailures.put(DeleteWorkspaceAuthzStep.class.getName(), StepStatus.STEP_RESULT_FAILURE_RETRY);
    doFailures.put(DeleteWorkspaceStateStep.class.getName(), StepStatus.STEP_RESULT_FAILURE_RETRY);
    FlightDebugInfo debugInfo = FlightDebugInfo.newBuilder().doStepFailures(doFailures).build();
    jobService.setFlightDebugInfoForTest(debugInfo);

    workspaceService.deleteWorkspace(request.workspaceId(), USER_REQUEST);
    assertThrows(
        WorkspaceNotFoundException.class,
        () -> workspaceService.getWorkspace(request.workspaceId(), USER_REQUEST));
  }

  @Test
  void deleteWorkspaceUndoFailure() {
    WorkspaceRequest request =
        defaultRequestBuilder(UUID.randomUUID())
            .workspaceStage(WorkspaceStage.MC_WORKSPACE)
            .build();
    workspaceService.createWorkspace(request, USER_REQUEST);

    // Test "undo" flight with lastStepFailure. Because deletion cannot be undone, this workspace
    // will still be deleted after "undoing" the flight.
    FlightDebugInfo debugInfo = FlightDebugInfo.newBuilder().lastStepFailure(true).build();
    jobService.setFlightDebugInfoForTest(debugInfo);

    // When a flight fails with no error message (e.g. because of debugInfo), Stairway will return
    // an InvalidResultStateException.
    assertThrows(
        InvalidResultStateException.class,
        () -> workspaceService.deleteWorkspace(request.workspaceId(), USER_REQUEST));

    // Even though the "undo" ran for this flight, the workspace should still be deleted.
    assertThrows(
        WorkspaceNotFoundException.class,
        () -> workspaceService.getWorkspace(request.workspaceId(), USER_REQUEST));
  }

  @Test
  void deleteForbiddenMissingWorkspace() throws Exception {
    doThrow(new UnauthorizedException("forbid!"))
        .when(mockSamService)
        .checkAuthz(any(), any(), any(), any());

    assertThrows(
        WorkspaceNotFoundException.class,
        () -> workspaceService.deleteWorkspace(UUID.randomUUID(), USER_REQUEST));
  }

  @Test
  void deleteForbiddenExistingWorkspace() throws Exception {
    WorkspaceRequest request = defaultRequestBuilder(UUID.randomUUID()).build();
    workspaceService.createWorkspace(request, USER_REQUEST);

    doThrow(new UnauthorizedException("forbid!"))
        .when(mockSamService)
        .checkAuthz(any(), any(), any(), any());

    assertThrows(
        UnauthorizedException.class,
        () -> workspaceService.deleteWorkspace(request.workspaceId(), USER_REQUEST));
  }

  @Test
  void deleteWorkspaceWithDataReference() {
    // First, create a workspace.
    UUID workspaceId = UUID.randomUUID();
    WorkspaceRequest request = defaultRequestBuilder(workspaceId).build();
    workspaceService.createWorkspace(request, USER_REQUEST);

    // Next, add a data reference to that workspace.
    UUID resourceId = UUID.randomUUID();
    ReferencedDataRepoSnapshotResource snapshot =
        new ReferencedDataRepoSnapshotResource(
            workspaceId,
            resourceId,
            "fake_data_reference",
            null,
            CloningInstructions.COPY_NOTHING,
            "fakeinstance",
            "fakesnapshot");
    referenceResourceService.createReferenceResource(snapshot, USER_REQUEST);

    // Validate that the reference exists.
    referenceResourceService.getReferenceResource(workspaceId, resourceId, USER_REQUEST);

    // Delete the workspace.
    workspaceService.deleteWorkspace(request.workspaceId(), USER_REQUEST);

    // Verify that the workspace was successfully deleted, even though it contained references
    assertThrows(
        WorkspaceNotFoundException.class,
        () -> workspaceService.getWorkspace(workspaceId, USER_REQUEST));

    // Verify that the resource is also deleted
    assertThrows(
        ResourceNotFoundException.class, () -> resourceDao.getResource(workspaceId, resourceId));
  }

  @Test
  @DisabledIfEnvironmentVariable(named = "TEST_ENV", matches = BUFFER_SERVICE_DISABLED_ENVS_REG_EX)
  void deleteWorkspaceWithGoogleContext() throws Exception {
    WorkspaceRequest request =
        defaultRequestBuilder(UUID.randomUUID())
            .spendProfileId(Optional.of(spendUtils.defaultSpendId()))
            .workspaceStage(WorkspaceStage.MC_WORKSPACE)
            .build();
    workspaceService.createWorkspace(request, USER_REQUEST);
    String jobId = UUID.randomUUID().toString();
    workspaceService.createGcpCloudContext(
        request.workspaceId(), jobId, USER_REQUEST, "/fake/value");
    jobService.waitForJob(jobId);
    assertNull(jobService.retrieveJobResult(jobId, Object.class, USER_REQUEST).getException());
    Workspace workspace = workspaceService.getWorkspace(request.workspaceId(), USER_REQUEST);
    String projectId =
        workspace.getGcpCloudContext().map(GcpCloudContext::getGcpProjectId).orElse(null);
    assertNotNull(projectId);

    // Verify project exists by retrieving it.
    crl.getCloudResourceManagerCow().projects().get(projectId).execute();

    workspaceService.deleteWorkspace(request.workspaceId(), USER_REQUEST);

    // Check that project is now being deleted.
    Project project = crl.getCloudResourceManagerCow().projects().get(projectId).execute();
    assertEquals("DELETE_REQUESTED", project.getState());
  }

  @Test
  @DisabledIfEnvironmentVariable(named = "TEST_ENV", matches = BUFFER_SERVICE_DISABLED_ENVS_REG_EX)
  void createGetDeleteGoogleContext() {
    WorkspaceRequest request =
        defaultRequestBuilder(UUID.randomUUID())
            .spendProfileId(Optional.of(spendUtils.defaultSpendId()))
            .workspaceStage(WorkspaceStage.MC_WORKSPACE)
            .build();
    workspaceService.createWorkspace(request, USER_REQUEST);

    String jobId = UUID.randomUUID().toString();
    workspaceService.createGcpCloudContext(
        request.workspaceId(), jobId, USER_REQUEST, "/fake/value");
    jobService.waitForJob(jobId);
    assertNull(jobService.retrieveJobResult(jobId, Object.class, USER_REQUEST).getException());
    Workspace workspace = workspaceService.getWorkspace(request.workspaceId(), USER_REQUEST);
    assertTrue(workspace.getGcpCloudContext().isPresent());

    workspaceService.deleteGcpCloudContext(request.workspaceId(), USER_REQUEST);
    workspace = workspaceService.getWorkspace(request.workspaceId(), USER_REQUEST);
    assertTrue(workspace.getGcpCloudContext().isEmpty());
  }

  @Test
  void createGoogleContextRawlsStageThrows() throws Exception {
    // RAWLS_WORKSPACE stage workspaces use existing Sam resources instead of owning them, so the
    // mock pretends our user has access to any workspace we ask about.
    Mockito.when(
            mockSamService.isAuthorized(
                Mockito.any(),
                Mockito.eq(SamConstants.SAM_WORKSPACE_RESOURCE),
                Mockito.any(),
                Mockito.eq(SamConstants.SAM_WORKSPACE_READ_ACTION)))
        .thenReturn(true);
    WorkspaceRequest request =
        defaultRequestBuilder(UUID.randomUUID())
            .workspaceStage(WorkspaceStage.RAWLS_WORKSPACE)
            .build();
    workspaceService.createWorkspace(request, USER_REQUEST);
    String jobId = UUID.randomUUID().toString();

    assertThrows(
        StageDisabledException.class,
        () ->
            workspaceService.createGcpCloudContext(
                request.workspaceId(), jobId, USER_REQUEST, "/fake/value"));
  }

  @Test
  void createGoogleContextNoSpendProfileIdThrows() {
    WorkspaceRequest request =
        defaultRequestBuilder(UUID.randomUUID())
            .workspaceStage(WorkspaceStage.MC_WORKSPACE)
            .build();
    workspaceService.createWorkspace(request, USER_REQUEST);
    String jobId = UUID.randomUUID().toString();

    assertThrows(
        MissingSpendProfileException.class,
        () ->
            workspaceService.createGcpCloudContext(
                request.workspaceId(), jobId, USER_REQUEST, "/fake/value"));
  }

  @Test
  void createGoogleContextSpendLinkingUnauthorizedThrows() throws Exception {
    WorkspaceRequest request =
        defaultRequestBuilder(UUID.randomUUID())
            .spendProfileId(Optional.of(spendUtils.defaultSpendId()))
            .workspaceStage(WorkspaceStage.MC_WORKSPACE)
            .build();
    workspaceService.createWorkspace(request, USER_REQUEST);
    String jobId = UUID.randomUUID().toString();

    Mockito.when(
            mockSamService.isAuthorized(
                Mockito.eq(USER_REQUEST),
                Mockito.eq(SamConstants.SPEND_PROFILE_RESOURCE),
                Mockito.any(),
                Mockito.eq(SamConstants.SPEND_PROFILE_LINK_ACTION)))
        .thenReturn(false);

    assertThrows(
        SpendUnauthorizedException.class,
        () ->
            workspaceService.createGcpCloudContext(
                request.workspaceId(), jobId, USER_REQUEST, "/fake/value"));
  }

  @Test
  void createGoogleContextSpendWithoutBillingAccountThrows() {
    WorkspaceRequest request =
        defaultRequestBuilder(UUID.randomUUID())
            .spendProfileId(Optional.of(spendUtils.noBillingAccount()))
            .workspaceStage(WorkspaceStage.MC_WORKSPACE)
            .build();
    workspaceService.createWorkspace(request, USER_REQUEST);
    String jobId = UUID.randomUUID().toString();

    assertThrows(
        NoBillingAccountException.class,
        () ->
            workspaceService.createGcpCloudContext(
                request.workspaceId(), jobId, USER_REQUEST, "/fake/value"));
  }

  /**
   * Convenience method for getting a WorkspaceRequest builder with some pre-filled default values.
   *
   * <p>This provides default values for jobId (random UUID), spend profile (Optional.empty()), and
   * workspace stage (MC_WORKSPACE).
   */
  private WorkspaceRequest.Builder defaultRequestBuilder(UUID workspaceId) {
    return WorkspaceRequest.builder()
        .workspaceId(workspaceId)
        .spendProfileId(Optional.empty())
        .workspaceStage(WorkspaceStage.MC_WORKSPACE);
  }
}
