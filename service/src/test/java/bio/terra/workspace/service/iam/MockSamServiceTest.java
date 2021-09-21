package bio.terra.workspace.service.iam;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;

import bio.terra.common.exception.UnauthorizedException;
import bio.terra.common.sam.exception.SamBadRequestException;
import bio.terra.workspace.common.AzureTestUtils;
import bio.terra.workspace.common.BaseAzureTest;
import bio.terra.workspace.common.fixtures.ReferenceResourceFixtures;
import bio.terra.workspace.db.exception.WorkspaceNotFoundException;
import bio.terra.workspace.service.datarepo.DataRepoService;
import bio.terra.workspace.service.iam.model.RoleBinding;
import bio.terra.workspace.service.iam.model.WsmIamRole;
import bio.terra.workspace.service.resource.controlled.AccessScopeType;
import bio.terra.workspace.service.resource.controlled.ControlledGcsBucketResource;
import bio.terra.workspace.service.resource.controlled.ManagedByType;
import bio.terra.workspace.service.resource.model.CloningInstructions;
import bio.terra.workspace.service.resource.referenced.ReferencedDataRepoSnapshotResource;
import bio.terra.workspace.service.resource.referenced.ReferencedResource;
import bio.terra.workspace.service.resource.referenced.ReferencedResourceService;
import bio.terra.workspace.service.workspace.WorkspaceService;
import bio.terra.workspace.service.workspace.exceptions.StageDisabledException;
import bio.terra.workspace.service.workspace.model.Workspace;
import bio.terra.workspace.service.workspace.model.WorkspaceRequest;
import bio.terra.workspace.service.workspace.model.WorkspaceStage;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;

// This is a pruned copy of the SamServiceTest setup to take the MockSamService path.
// It is really a Unit test.
@ActiveProfiles("azure")
class MockSamServiceTest extends BaseAzureTest {

  @Autowired private SamService samService;
  @Autowired private WorkspaceService workspaceService;
  @Autowired private ReferencedResourceService referenceResourceService;
  @Autowired private AzureTestUtils azureTestUtils;

  @MockBean private DataRepoService mockDataRepoService;

  @BeforeEach
  public void setup() {
    doReturn(true).when(mockDataRepoService).snapshotReadable(any(), any(), any());
    azureTestUtils.loadUsersFromConfiguration();
  }

  @Test
  void addedReaderCanRead() throws Exception {
    UUID workspaceId = createWorkspaceDefaultUser();
    // Before being granted permission, secondary user should be rejected.
    assertThrows(
        UnauthorizedException.class,
        () -> workspaceService.getWorkspace(workspaceId, secondaryUserRequest()));
    // After being granted permission, secondary user can read the workspace.
    samService.grantWorkspaceRole(
        workspaceId, defaultUserRequest(), WsmIamRole.READER, secondaryUserEmail());
    Workspace readWorkspace = workspaceService.getWorkspace(workspaceId, secondaryUserRequest());
    assertEquals(workspaceId, readWorkspace.getWorkspaceId());
  }

  @Test
  void addedWriterCanWrite() throws Exception {
    UUID workspaceId = createWorkspaceDefaultUser();

    ReferencedDataRepoSnapshotResource referenceResource =
        ReferenceResourceFixtures.makeDataRepoSnapshotResource(workspaceId);

    // Before being granted permission, secondary user should be rejected.
    assertThrows(
        UnauthorizedException.class,
        () ->
            referenceResourceService.createReferenceResource(
                referenceResource, secondaryUserRequest()));

    // After being granted permission, secondary user can modify the workspace.
    samService.grantWorkspaceRole(
        workspaceId, defaultUserRequest(), WsmIamRole.WRITER, secondaryUserEmail());

    ReferencedResource ref =
        referenceResourceService.createReferenceResource(referenceResource, secondaryUserRequest());
    ReferencedDataRepoSnapshotResource resultResource = ref.castToDataRepoSnapshotResource();
    assertEquals(referenceResource, resultResource);
  }

  @Test
  void removedReaderCannotRead() throws Exception {
    UUID workspaceId = createWorkspaceDefaultUser();
    // Before being granted permission, secondary user should be rejected.
    assertThrows(
        UnauthorizedException.class,
        () -> workspaceService.getWorkspace(workspaceId, secondaryUserRequest()));
    // After being granted permission, secondary user can read the workspace.
    samService.grantWorkspaceRole(
        workspaceId, defaultUserRequest(), WsmIamRole.READER, secondaryUserEmail());
    Workspace readWorkspace = workspaceService.getWorkspace(workspaceId, secondaryUserRequest());
    assertEquals(workspaceId, readWorkspace.getWorkspaceId());
    // After removing permission, secondary user can no longer read.
    samService.removeWorkspaceRole(
        workspaceId, defaultUserRequest(), WsmIamRole.READER, secondaryUserEmail());
    assertThrows(
        UnauthorizedException.class,
        () -> workspaceService.getWorkspace(workspaceId, secondaryUserRequest()));
  }

  @Test
  void nonOwnerCannotAddReader() {
    UUID workspaceId = createWorkspaceDefaultUser();
    // Note that this request uses the secondary user's authentication token, when only the first
    // user is an owner.
    assertThrows(
        UnauthorizedException.class,
        () ->
            samService.grantWorkspaceRole(
                workspaceId, secondaryUserRequest(), WsmIamRole.READER, secondaryUserEmail()));
  }

  @Test
  void permissionsApiFailsInRawlsWorkspace() throws Exception {
    UUID workspaceId = UUID.randomUUID();
    // RAWLS_WORKSPACEs do not own their own Sam resources, so we need to manage them separately.
    samService.createWorkspaceWithDefaults(defaultUserRequest(), workspaceId);

    WorkspaceRequest rawlsRequest =
        WorkspaceRequest.builder()
            .workspaceId(workspaceId)
            .workspaceStage(WorkspaceStage.RAWLS_WORKSPACE)
            .build();
    workspaceService.createWorkspace(rawlsRequest, defaultUserRequest());
    assertThrows(
        StageDisabledException.class,
        () ->
            samService.grantWorkspaceRole(
                workspaceId, defaultUserRequest(), WsmIamRole.READER, secondaryUserEmail()));

    samService.deleteWorkspace(defaultUserRequest(), workspaceId);
  }

  @Test
  void invalidUserEmailRejected() {
    UUID workspaceId = createWorkspaceDefaultUser();
    assertThrows(
        SamBadRequestException.class,
        () ->
            samService.grantWorkspaceRole(
                workspaceId,
                defaultUserRequest(),
                WsmIamRole.READER,
                "!!!INVALID EMAIL ADDRESS!!!!"));
  }

  @Test
  void listPermissionsIncludesAddedUsers() throws Exception {
    UUID workspaceId = createWorkspaceDefaultUser();
    samService.grantWorkspaceRole(
        workspaceId, defaultUserRequest(), WsmIamRole.READER, secondaryUserEmail());
    List<RoleBinding> policyList = samService.listRoleBindings(workspaceId, defaultUserRequest());

    RoleBinding expectedOwnerBinding =
        RoleBinding.builder()
            .role(WsmIamRole.OWNER)
            .users(Collections.singletonList(defaultUserEmail()))
            .build();
    RoleBinding expectedReaderBinding =
        RoleBinding.builder()
            .role(WsmIamRole.READER)
            .users(Collections.singletonList(secondaryUserEmail()))
            .build();
    RoleBinding expectedWriterBinding =
        RoleBinding.builder().role(WsmIamRole.WRITER).users(Collections.emptyList()).build();
    RoleBinding expectedApplicationBinding =
        RoleBinding.builder().role(WsmIamRole.APPLICATION).users(Collections.emptyList()).build();
    assertThat(
        policyList,
        containsInAnyOrder(
            equalTo(expectedOwnerBinding),
            equalTo(expectedWriterBinding),
            equalTo(expectedReaderBinding),
            equalTo(expectedApplicationBinding)));
  }

  @Test
  void writerCannotListPermissions() throws Exception {
    UUID workspaceId = createWorkspaceDefaultUser();
    samService.grantWorkspaceRole(
        workspaceId, defaultUserRequest(), WsmIamRole.WRITER, secondaryUserEmail());
    assertThrows(
        UnauthorizedException.class,
        () -> samService.listRoleBindings(workspaceId, secondaryUserRequest()));
  }

  @Test
  void grantRoleInMissingWorkspaceThrows() {
    UUID fakeId = UUID.randomUUID();
    assertThrows(
        WorkspaceNotFoundException.class,
        () ->
            samService.grantWorkspaceRole(
                fakeId, defaultUserRequest(), WsmIamRole.READER, secondaryUserEmail()));
  }

  @Test
  void readRolesInMissingWorkspaceThrows() {
    UUID fakeId = UUID.randomUUID();
    assertThrows(
        WorkspaceNotFoundException.class,
        () -> samService.listRoleBindings(fakeId, defaultUserRequest()));
  }

  @Test
  void listWorkspacesIncludesWsmWorkspace() throws Exception {
    UUID workspaceId = createWorkspaceDefaultUser();
    List<UUID> samWorkspaceIdList = samService.listWorkspaceIds(defaultUserRequest());
    assertTrue(samWorkspaceIdList.contains(workspaceId));
  }

  // TODO: these tests all require
  /*
    @Test
    void workspaceReaderIsSharedResourceReader() throws Exception {
      // Default user is workspace owner, secondary user is workspace reader
      UUID workspaceId = createWorkspaceDefaultUser();
      samService.grantWorkspaceRole(
          workspaceId, defaultUserRequest(), WsmIamRole.READER, secondaryUserEmail());

      ControlledResource bucketResource = defaultBucket(workspaceId).build();
      samService.createControlledResource(bucketResource, null, defaultUserRequest());

      // Workspace reader should have read access on a user-shared resource via inheritance
      assertTrue(
          samService.isAuthorized(
              secondaryUserRequest(),
              ControlledResourceCategory.USER_SHARED.getSamResourceName(),
              bucketResource.getResourceId().toString(),
              SamConstants.SAM_WORKSPACE_READ_ACTION));

      samService.deleteControlledResource(bucketResource, defaultUserRequest());
    }

    @Test
    void workspaceReaderIsNotPrivateResourceReader() throws Exception {
      // Default user is workspace owner, secondary user is workspace reader
      UUID workspaceId = createWorkspaceDefaultUser();
      samService.grantWorkspaceRole(
          workspaceId, defaultUserRequest(), WsmIamRole.READER, secondaryUserEmail());

      // Create private resource assigned to the default user.
      ControlledResource bucketResource =
          defaultBucket(workspaceId)
              .accessScope(AccessScopeType.ACCESS_SCOPE_PRIVATE)
              .assignedUser(defaultUserEmail())
              .build();
      List<ControlledResourceIamRole> privateResourceIamRoles =
          ImmutableList.of(ControlledResourceIamRole.READER, ControlledResourceIamRole.EDITOR);
      samService.createControlledResource(
          bucketResource, privateResourceIamRoles, defaultUserRequest());

      // Workspace reader should not have read access on a private resource.
      assertFalse(
          samService.isAuthorized(
              secondaryUserRequest(),
              ControlledResourceCategory.USER_PRIVATE.getSamResourceName(),
              bucketResource.getResourceId().toString(),
              SamConstants.SAM_WORKSPACE_READ_ACTION));
      // However, the assigned user should have read access.
      assertTrue(
          samService.isAuthorized(
              defaultUserRequest(),
              ControlledResourceCategory.USER_PRIVATE.getSamResourceName(),
              bucketResource.getResourceId().toString(),
              SamConstants.SAM_WORKSPACE_READ_ACTION));

      samService.deleteControlledResource(bucketResource, defaultUserRequest());
    }

    @Test
    void duplicateResourceCreateIgnored() throws Exception {
      UUID workspaceId = createWorkspaceDefaultUser();

      ControlledResource bucketResource = defaultBucket(workspaceId).build();
      samService.createControlledResource(bucketResource, null, defaultUserRequest());
      // This duplicate call should complete without throwing.
      samService.createControlledResource(bucketResource, null, defaultUserRequest());
    }

    @Test
    void duplicateResourceDeleteIgnored() throws Exception {
      UUID workspaceId = createWorkspaceDefaultUser();

      ControlledResource bucketResource = defaultBucket(workspaceId).build();
      samService.createControlledResource(bucketResource, null, defaultUserRequest());

      samService.deleteControlledResource(bucketResource, defaultUserRequest());
      samService.deleteControlledResource(bucketResource, defaultUserRequest());
    }
  */

  // Convenience methods for accessing default and secondary users.
  private AuthenticatedUserRequest defaultUserRequest() {
    return azureTestUtils.getUserRequest(0);
  }

  private AuthenticatedUserRequest secondaryUserRequest() {
    return azureTestUtils.getUserRequest(1);
  }

  private String defaultUserEmail() {
    return azureTestUtils.getTestUserEmail(0);
  }

  private String secondaryUserEmail() {
    return azureTestUtils.getTestUserEmail(1);
  }

  /** Create a workspace using the default test user for connected tests, return its ID. */
  private UUID createWorkspaceDefaultUser() {
    return createWorkspaceForUser(defaultUserRequest());
  }

  private UUID createWorkspaceForUser(AuthenticatedUserRequest userRequest) {
    WorkspaceRequest request =
        WorkspaceRequest.builder()
            .workspaceId(UUID.randomUUID())
            .workspaceStage(WorkspaceStage.MC_WORKSPACE)
            .build();
    return workspaceService.createWorkspace(request, userRequest);
  }

  /**
   * Creates a controlled user-shared GCS bucket with random resource ID and constant name, bucket
   * name, and cloning instructions.
   */
  private ControlledGcsBucketResource.Builder defaultBucket(UUID workspaceId) {
    return ControlledGcsBucketResource.builder()
        .workspaceId(workspaceId)
        .resourceId(UUID.randomUUID())
        .bucketName("fake-bucket-name")
        .name("fakeResourceName")
        .cloningInstructions(CloningInstructions.COPY_NOTHING)
        .accessScope(AccessScopeType.ACCESS_SCOPE_SHARED)
        .managedBy(ManagedByType.MANAGED_BY_USER);
  }
}
