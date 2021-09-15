package bio.terra.workspace.service;

import bio.terra.workspace.app.configuration.external.AzureState;
import bio.terra.workspace.db.IamDao;
import bio.terra.workspace.db.IamDao.PocUser;
import bio.terra.workspace.service.iam.AuthenticatedUserRequest;
import bio.terra.workspace.service.iam.AuthenticatedUserRequest.AuthType;
import bio.terra.workspace.service.iam.model.SamConstants;
import bio.terra.workspace.service.iam.model.SamConstants.SamControlledResourceActions;
import bio.terra.workspace.service.iam.model.WsmIamRole;
import bio.terra.workspace.service.resource.controlled.ControlledResource;
import bio.terra.workspace.service.resource.controlled.ControlledResourceCategory;
import java.util.List;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class MockSamService {
  private final IamDao iamDao;
  private final AzureState azureState;

  @Autowired
  public MockSamService(IamDao iamDao, AzureState azureState) {
    this.iamDao = iamDao;
    this.azureState = azureState;
  }

  /**
   * Helper method to decide whether or not to use the mock.
   *
   * @param userRequest incoming user auth
   * @return true to use the mock
   */
  public boolean useMock(AuthenticatedUserRequest userRequest) {
    return (azureState.isEnabled() && userRequest.getAuthType() == AuthType.BASIC);
  }

  public void createWorkspaceWithDefaults(AuthenticatedUserRequest userRequest, UUID id) {
    // Grant the user owner on the resource
    iamDao.grantRole(id, WsmIamRole.OWNER, new PocUser(userRequest));
  }

  public void deleteControlledResource(
      ControlledResource resource, AuthenticatedUserRequest userRequest) {
    // Mock does not maintain any state for controlled resources
  }

  public void deleteWorkspace(AuthenticatedUserRequest userRequest, UUID id) {
    // I don't think we need an access check here. There is one in the logic already.
    iamDao.deleteWorkspace(id);
  }

  public List<UUID> listWorkspaceIds(AuthenticatedUserRequest userRequest) {
    return iamDao.listAccessible(new PocUser(userRequest));
  }

  public boolean isAuthorized(
      AuthenticatedUserRequest userRequest,
      String iamResourceType,
      String resourceId,
      String action) {

    String userId = userRequest.getSubjectId();

    // SpendProfile - everyone has access. Wheeeee!
    if (StringUtils.equals(iamResourceType, SamConstants.SPEND_PROFILE_RESOURCE)) {
      return true;
    }

    // Workspace - map action to role checks in the database
    if (StringUtils.equals(iamResourceType, SamConstants.SAM_WORKSPACE_RESOURCE)) {
      UUID workspaceId = UUID.fromString(resourceId);
      switch (action) {
          // workspace owner actions
        case SamConstants.SAM_WORKSPACE_OWN_ACTION:
        case SamConstants.SAM_WORKSPACE_READ_IAM_ACTION:
        case SamConstants.SAM_WORKSPACE_DELETE_ACTION:
          return iamDao.roleCheck(workspaceId, List.of(WsmIamRole.OWNER), userId);

          // workspace owner or writer actions
        case SamConstants.SAM_WORKSPACE_WRITE_ACTION:
          return iamDao.roleCheck(
              workspaceId, List.of(WsmIamRole.OWNER, WsmIamRole.WRITER), userId);

          // workspace reader actions
        case SamConstants.SAM_WORKSPACE_READ_ACTION:
          return iamDao.roleCheck(
              workspaceId, List.of(WsmIamRole.OWNER, WsmIamRole.WRITER, WsmIamRole.READER), userId);

        default:
          throw new IllegalStateException("Unexpected action on workspace: " + action);
      }
    }

    // User Shared Resource - map action to workspace roles
    // Workspace OWNER and WRITER -> editor/writer/reader role on resource = all actions on resource
    // Workspace READER -> reader role on resource = read action on resource
    if (StringUtils.equals(
        iamResourceType, ControlledResourceCategory.USER_SHARED.getSamResourceName())) {

      UUID workspaceId =
          iamDao
              .getWorkspaceIdFromResourceId(resourceId)
              .orElseThrow(() -> new IllegalArgumentException("unknown resource id"));

      switch (action) {
        case SamControlledResourceActions.EDIT_ACTION:
        case SamControlledResourceActions.DELETE_ACTION:
        case SamControlledResourceActions.WRITE_ACTION:
          return iamDao.roleCheck(
              workspaceId, List.of(WsmIamRole.OWNER, WsmIamRole.WRITER), userId);

        case SamControlledResourceActions.READ_ACTION:
          return iamDao.roleCheck(
              workspaceId, List.of(WsmIamRole.OWNER, WsmIamRole.WRITER, WsmIamRole.READER), userId);

        default:
          throw new IllegalStateException("Unexpected action on resource: " + action);
      }
    }

    // For now, we do not support other resource types in the Azure PoC
    throw new IllegalStateException("We only support user shared resources in the PoC right now");
  }
}
