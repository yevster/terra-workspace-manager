package bio.terra.workspace.service.iam;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;

import bio.terra.common.exception.UnauthorizedException;
import bio.terra.common.sam.exception.SamBadRequestException;
import bio.terra.workspace.common.BaseConnectedTest;
import bio.terra.workspace.common.fixtures.ReferenceResourceFixtures;
import bio.terra.workspace.connected.UserAccessUtils;
import bio.terra.workspace.db.exception.WorkspaceNotFoundException;
import bio.terra.workspace.service.datarepo.DataRepoService;
import bio.terra.workspace.service.iam.model.ControlledResourceIamRole;
import bio.terra.workspace.service.iam.model.RoleBinding;
import bio.terra.workspace.service.iam.model.SamConstants;
import bio.terra.workspace.service.iam.model.WsmIamRole;
import bio.terra.workspace.service.resource.controlled.AccessScopeType;
import bio.terra.workspace.service.resource.controlled.ControlledGcsBucketResource;
import bio.terra.workspace.service.resource.controlled.ControlledResource;
import bio.terra.workspace.service.resource.controlled.ControlledResourceCategory;
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
import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;

class SamServiceTest extends BaseConnectedTest {

  @Autowired private SamService samService;
  @Autowired private WorkspaceService workspaceService;
  @Autowired private UserAccessUtils userAccessUtils;
  @Autowired private ReferencedResourceService referenceResourceService;

  @MockBean private DataRepoService mockDataRepoService;

  @BeforeEach
  public void setup() {
    doReturn(true).when(mockDataRepoService).snapshotReadable(any(), any(), any());
  }

  @Test
  void samServiceInitializationSucceeded() throws Exception {
    // As part of initialization, WSM's service account is registered as a user in Sam. This test
    // verifies that registration status by calling Sam directly.
    String wsmAccessToken;
    wsmAccessToken = samService.getWsmServiceAccountToken();
    assertTrue(samService.wsmServiceAccountRegistered(samService.samUsersApi(wsmAccessToken)));
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
        workspaceId, defaultUserRequest(), WsmIamRole.READER, userAccessUtils.getSecondUserEmail());
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
        workspaceId, defaultUserRequest(), WsmIamRole.WRITER, userAccessUtils.getSecondUserEmail());

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
        workspaceId, defaultUserRequest(), WsmIamRole.READER, userAccessUtils.getSecondUserEmail());
    Workspace readWorkspace = workspaceService.getWorkspace(workspaceId, secondaryUserRequest());
    assertEquals(workspaceId, readWorkspace.getWorkspaceId());
    // After removing permission, secondary user can no longer read.
    samService.removeWorkspaceRole(
        workspaceId, defaultUserRequest(), WsmIamRole.READER, userAccessUtils.getSecondUserEmail());
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
                workspaceId,
                secondaryUserRequest(),
                WsmIamRole.READER,
                userAccessUtils.getSecondUserEmail()));
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
                workspaceId,
                defaultUserRequest(),
                WsmIamRole.READER,
                userAccessUtils.getSecondUserEmail()));

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
        workspaceId, defaultUserRequest(), WsmIamRole.READER, userAccessUtils.getSecondUserEmail());
    List<RoleBinding> policyList = samService.listRoleBindings(workspaceId, defaultUserRequest());

    RoleBinding expectedOwnerBinding =
        RoleBinding.builder()
            .role(WsmIamRole.OWNER)
            .users(Collections.singletonList(userAccessUtils.getDefaultUserEmail()))
            .build();
    RoleBinding expectedReaderBinding =
        RoleBinding.builder()
            .role(WsmIamRole.READER)
            .users(Collections.singletonList(userAccessUtils.getSecondUserEmail()))
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
        workspaceId, defaultUserRequest(), WsmIamRole.WRITER, userAccessUtils.getSecondUserEmail());
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
                fakeId,
                defaultUserRequest(),
                WsmIamRole.READER,
                userAccessUtils.getSecondUserEmail()));
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
    List<UUID> samWorkspaceIdList =
        samService.listWorkspaceIds(userAccessUtils.defaultUserAuthRequest());
    assertTrue(samWorkspaceIdList.contains(workspaceId));
  }

  @Test
  void workspaceReaderIsSharedResourceReader() throws Exception {
    // Default user is workspace owner, secondary user is workspace reader
    UUID workspaceId = createWorkspaceDefaultUser();
    samService.grantWorkspaceRole(
        workspaceId, defaultUserRequest(), WsmIamRole.READER, userAccessUtils.getSecondUserEmail());

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
        workspaceId, defaultUserRequest(), WsmIamRole.READER, userAccessUtils.getSecondUserEmail());

    // Create private resource assigned to the default user.
    ControlledResource bucketResource =
        defaultBucket(workspaceId)
            .accessScope(AccessScopeType.ACCESS_SCOPE_PRIVATE)
            .assignedUser(userAccessUtils.getDefaultUserEmail())
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

  /**
   * Convenience method to build an AuthenticatedUserRequest from utils' default user.
   *
   * <p>This only fills in access token, not email or subjectId.
   */
  private AuthenticatedUserRequest defaultUserRequest() {
    return new AuthenticatedUserRequest(
        null, null, Optional.of(userAccessUtils.defaultUserAccessToken().getTokenValue()));
  }

  /**
   * Convenience method to build an AuthenticatedUserRequest from utils' secondary default user.
   *
   * <p>This only fills in access token, not email or subjectId.
   */
  private AuthenticatedUserRequest secondaryUserRequest() {
    return new AuthenticatedUserRequest(
        null, null, Optional.of(userAccessUtils.secondUserAccessToken().getTokenValue()));
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
