package bio.terra.workspace.service.iam;

import bio.terra.common.exception.InternalServerErrorException;
import bio.terra.common.exception.UnauthorizedException;
import bio.terra.common.sam.SamRetry;
import bio.terra.common.sam.exception.SamExceptionFactory;
import bio.terra.workspace.app.configuration.external.SamConfiguration;
import bio.terra.workspace.service.iam.model.ControlledResourceIamRole;
import bio.terra.workspace.service.iam.model.RoleBinding;
import bio.terra.workspace.service.iam.model.SamConstants;
import bio.terra.workspace.service.iam.model.WsmIamRole;
import bio.terra.workspace.service.resource.controlled.AccessScopeType;
import bio.terra.workspace.service.resource.controlled.ControlledResource;
import bio.terra.workspace.service.stage.StageService;
import bio.terra.workspace.service.workspace.exceptions.InternalLogicException;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import io.opencensus.contrib.spring.aop.Traced;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.broadinstitute.dsde.workbench.client.sam.ApiClient;
import org.broadinstitute.dsde.workbench.client.sam.ApiException;
import org.broadinstitute.dsde.workbench.client.sam.api.GoogleApi;
import org.broadinstitute.dsde.workbench.client.sam.api.ResourcesApi;
import org.broadinstitute.dsde.workbench.client.sam.api.StatusApi;
import org.broadinstitute.dsde.workbench.client.sam.api.UsersApi;
import org.broadinstitute.dsde.workbench.client.sam.model.AccessPolicyMembershipV2;
import org.broadinstitute.dsde.workbench.client.sam.model.AccessPolicyResponseEntry;
import org.broadinstitute.dsde.workbench.client.sam.model.AccessPolicyResponseEntryV2;
import org.broadinstitute.dsde.workbench.client.sam.model.CreateResourceRequestV2;
import org.broadinstitute.dsde.workbench.client.sam.model.FullyQualifiedResourceId;
import org.broadinstitute.dsde.workbench.client.sam.model.ResourceAndAccessPolicy;
import org.broadinstitute.dsde.workbench.client.sam.model.SystemStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

/**
 * SamService encapsulates logic for interacting with Sam. HTTP Statuses returned by Sam are
 * interpreted by the functions in this class.
 *
 * <p>This class is used both by Flights and outside of Flights. Flights need the
 * InterruptedExceptions to be thrown. Outside of flights, use the rethrowIfSamInterrupted. See
 * comment there for more detail.
 */
@Component
public class SamService {

  private final SamConfiguration samConfig;
  private final StageService stageService;

  private final Set<String> SAM_OAUTH_SCOPES = ImmutableSet.of("openid", "email", "profile");
  private boolean wsmServiceAccountInitialized;

  @Autowired
  public SamService(SamConfiguration samConfig, StageService stageService) {
    this.samConfig = samConfig;
    this.stageService = stageService;
    this.wsmServiceAccountInitialized = false;
  }

  private final Logger logger = LoggerFactory.getLogger(SamService.class);

  private ApiClient getApiClient(String accessToken) {
    ApiClient client = new ApiClient();
    client.setAccessToken(accessToken);
    return client.setBasePath(samConfig.getBasePath());
  }

  private ResourcesApi samResourcesApi(String accessToken) {
    return new ResourcesApi(getApiClient(accessToken));
  }

  private GoogleApi samGoogleApi(String accessToken) {
    return new GoogleApi(getApiClient(accessToken));
  }

  @VisibleForTesting
  public UsersApi samUsersApi(String accessToken) {
    return new UsersApi(getApiClient(accessToken));
  }

  @VisibleForTesting
  public String getWsmServiceAccountToken() throws IOException {
    GoogleCredentials creds =
        GoogleCredentials.getApplicationDefault().createScoped(SAM_OAUTH_SCOPES);
    creds.refreshIfExpired();
    return creds.getAccessToken().getTokenValue();
  }

  /**
   * Obtain the user email address from an AuthenticatedUserRequest either by the easy way (directly
   * calling getEmail()), or the harder way, calling Sam. The hard way throws a checked exception,
   * which we want to convert to an unchecked exception here.
   *
   * @param userRequest - request object for this user
   * @return - email address of user
   */
  public String getRequestUserEmail(AuthenticatedUserRequest userRequest)
      throws InterruptedException {
    Optional<String> emailMaybe = Optional.ofNullable(userRequest.getEmail());
    if (emailMaybe.isPresent()) {
      return emailMaybe.get();
    } else {
      return getEmailFromToken(userRequest.getRequiredToken());
    }
  }

  /**
   * Register WSM's service account as a user in Sam if it isn't already. This should only need to
   * register with Sam once per environment, so it is implemented lazily.
   */
  private void initializeWsmServiceAccount() throws InterruptedException {
    if (!wsmServiceAccountInitialized) {
      String wsmAccessToken = null;
      try {
        wsmAccessToken = getWsmServiceAccountToken();
      } catch (IOException e) {
        // In cases where WSM is not running as a service account (e.g. unit tests), the above call
        // will throw. This can be ignored now and later when the credentials are used again.
        logger.warn(
            "Failed to register WSM service account in Sam. This is expected for tests.", e);
        return;
      }
      UsersApi usersApi = samUsersApi(wsmAccessToken);
      // If registering the service account fails, all we can do is to keep trying.
      if (!wsmServiceAccountRegistered(usersApi)) {
        // retries internally
        registerWsmServiceAccount(usersApi);
      }
      wsmServiceAccountInitialized = true;
    }
  }

  @VisibleForTesting
  public boolean wsmServiceAccountRegistered(UsersApi usersApi) throws InterruptedException {
    try {
      // getUserStatusInfo throws a 404 if the calling user is not registered, which will happen
      // the first time WSM is run in each environment.
      SamRetry.retry(usersApi::getUserStatusInfo);
      logger.info("WSM service account already registered in Sam");
      return true;
    } catch (ApiException apiException) {
      if (apiException.getCode() == HttpStatus.NOT_FOUND.value()) {
        logger.info(
            "Sam error was NOT_FOUND when checking user registration. This means the "
                + " user is not registered but is not an exception. Returning false.");
        return false;
      } else {
        throw SamExceptionFactory.create("Error checking user status in Sam", apiException);
      }
    }
  }

  private void registerWsmServiceAccount(UsersApi usersApi) throws InterruptedException {
    try {
      SamRetry.retry(usersApi::createUserV2);
    } catch (ApiException apiException) {
      throw SamExceptionFactory.create(
          "Error registering WSM service account with Sam", apiException);
    }
  }

  /**
   * Wrapper around the Sam client to create a workspace resource in Sam.
   *
   * <p>This creates a workspace with the provided ID and requesting user as the sole Owner. Empty
   * reader and writer policies are also created. Errors from the Sam client will be thrown as Sam
   * specific exception types.
   */
  @Traced
  public void createWorkspaceWithDefaults(AuthenticatedUserRequest userRequest, UUID id)
      throws InterruptedException {
    ResourcesApi resourceApi = samResourcesApi(userRequest.getRequiredToken());
    // Sam will throw an error if no owner is specified, so the caller's email is required. It can
    // be looked up using the auth token if that's all the caller provides.
    String callerEmail =
        userRequest.getEmail() == null
            ? getEmailFromToken(userRequest.getRequiredToken())
            : userRequest.getEmail();
    CreateResourceRequestV2 workspaceRequest =
        new CreateResourceRequestV2()
            .resourceId(id.toString())
            .policies(defaultWorkspacePolicies(callerEmail));
    try {
      SamRetry.retry(
          () ->
              resourceApi.createResourceV2(SamConstants.SAM_WORKSPACE_RESOURCE, workspaceRequest));
      logger.info("Created Sam resource for workspace {}", id);
    } catch (ApiException apiException) {
      throw SamExceptionFactory.create("Error creating a Workspace resource in Sam", apiException);
    }
  }

  /**
   * List all workspace IDs in Sam this user has access to. Note that in environments shared with
   * Rawls, some of these workspaces will be Rawls managed and WSM will not know about them.
   */
  @Traced
  public List<UUID> listWorkspaceIds(AuthenticatedUserRequest userRequest)
      throws InterruptedException {
    ResourcesApi resourceApi = samResourcesApi(userRequest.getRequiredToken());
    List<UUID> workspaceIds = new ArrayList<>();
    try {
      List<ResourceAndAccessPolicy> resourceAndPolicies =
          SamRetry.retry(
              () -> resourceApi.listResourcesAndPolicies(SamConstants.SAM_WORKSPACE_RESOURCE));
      for (var resourceAndPolicy : resourceAndPolicies) {
        try {
          workspaceIds.add(UUID.fromString(resourceAndPolicy.getResourceId()));
        } catch (IllegalArgumentException e) {
          // WSM always uses UUIDs for workspace IDs, but this is not enforced in Sam and there are
          // old workspaces that don't use UUIDs. Any workspace with a non-UUID workspace ID is
          // ignored here.
          continue;
        }
      }
    } catch (ApiException apiException) {
      throw SamExceptionFactory.create("Error listing Workspace Ids in Sam", apiException);
    }
    return workspaceIds;
  }

  @Traced
  public void deleteWorkspace(AuthenticatedUserRequest userRequest, UUID id)
      throws InterruptedException {
    String authToken = userRequest.getRequiredToken();
    ResourcesApi resourceApi = samResourcesApi(authToken);
    try {
      SamRetry.retry(
          () -> resourceApi.deleteResource(SamConstants.SAM_WORKSPACE_RESOURCE, id.toString()));
      logger.info("Deleted Sam resource for workspace {}", id);
    } catch (ApiException apiException) {
      logger.info("Sam API error while deleting workspace, code is " + apiException.getCode());
      // Do nothing if the resource to delete is not found, this may not be the first time undo is
      // called. Other exceptions still need to be surfaced.
      if (apiException.getCode() == HttpStatus.NOT_FOUND.value()) {
        logger.info(
            "Sam error was NOT_FOUND on a deletion call. "
                + "This just means the deletion was tried twice so no error thrown.");
        return;
      }
      throw SamExceptionFactory.create("Error deleting a workspace in Sam", apiException);
    }
  }

  @Traced
  public boolean isAuthorized(
      AuthenticatedUserRequest userRequest,
      String iamResourceType,
      String resourceId,
      String action)
      throws InterruptedException {
    String accessToken = userRequest.getRequiredToken();
    ResourcesApi resourceApi = samResourcesApi(accessToken);
    try {
      return SamRetry.retry(
          () -> resourceApi.resourcePermissionV2(iamResourceType, resourceId, action));
    } catch (ApiException apiException) {
      throw SamExceptionFactory.create("Error checking resource permission in Sam", apiException);
    }
  }

  /**
   * Check whether a user may perform an action on a Sam resource. Unlike {@code isAuthorized}, this
   * method does not require that the calling user and the authenticating user are the same - e.g.
   * user A may ask Sam whether user B has permission to perform an action.
   *
   * @param iamResourceType The type of the Sam resource to check
   * @param resourceId The ID of the Sam resource to check
   * @param action The action we're querying Sam for
   * @param userToCheck The email of the principle whose permission we are checking
   * @param userRequest Credentials for the call to Sam. These do not need to be from the same user
   *     as userToCheck.
   * @return True if userToCheck may perform the specified action on the specified resource. False
   *     otherwise.
   */
  @Traced
  public boolean userIsAuthorized(
      String iamResourceType,
      String resourceId,
      String action,
      String userToCheck,
      AuthenticatedUserRequest userRequest)
      throws InterruptedException {
    ResourcesApi resourceApi = samResourcesApi(userRequest.getRequiredToken());
    try {
      return SamRetry.retry(
          () -> resourceApi.resourceActionV2(iamResourceType, resourceId, action, userToCheck));
    } catch (ApiException apiException) {
      throw SamExceptionFactory.create("Error checking resource permission in Sam", apiException);
    }
  }

  /**
   * Wrapper around isAuthorized which throws an appropriate exception if a user does not have
   * access to a resource. The wrapped call will perform a check for the appropriate permission in
   * Sam. This call answers the question "does user X have permission to do action Y on resource Z".
   *
   * @param userRequest Credentials of the user whose permissions are being checked
   * @param resourceType The Sam type of the resource being checked
   * @param resourceId The ID of the resource being checked
   * @param action The action being checked on the resource
   */
  @Traced
  public void checkAuthz(
      AuthenticatedUserRequest userRequest, String resourceType, String resourceId, String action)
      throws InterruptedException {
    boolean isAuthorized = isAuthorized(userRequest, resourceType, resourceId, action);
    final String userEmail = getEmailFromToken(userRequest.getRequiredToken());
    if (!isAuthorized)
      throw new UnauthorizedException(
          String.format(
              "User %s is not authorized to %s resource %s of type %s",
              userEmail, action, resourceId, resourceType));
    else
      logger.info(
          "User {} is authorized to {} resource {} of type {}",
          userEmail,
          action,
          resourceId,
          resourceType);
  }

  /**
   * Wrapper around Sam client to grant a role to the provided user.
   *
   * <p>This operation is only available to MC_WORKSPACE stage workspaces, as Rawls manages
   * permissions directly on other workspaces.
   *
   * @param workspaceId The workspace this operation takes place in
   * @param userRequest Credentials of the user requesting this operation. Only owners have
   *     permission to modify roles in a workspace.
   * @param role The role being granted.
   * @param email The user being granted a role.
   */
  @Traced
  public void grantWorkspaceRole(
      UUID workspaceId, AuthenticatedUserRequest userRequest, WsmIamRole role, String email)
      throws InterruptedException {
    stageService.assertMcWorkspace(workspaceId, "grantWorkspaceRole");
    checkAuthz(
        userRequest,
        SamConstants.SAM_WORKSPACE_RESOURCE,
        workspaceId.toString(),
        samActionToModifyRole(role));
    ResourcesApi resourceApi = samResourcesApi(userRequest.getRequiredToken());
    try {
      SamRetry.retry(
          () ->
              resourceApi.addUserToPolicy(
                  SamConstants.SAM_WORKSPACE_RESOURCE,
                  workspaceId.toString(),
                  role.toSamRole(),
                  email));
      logger.info(
          "Granted role {} to user {} in workspace {}", role.toSamRole(), email, workspaceId);
    } catch (ApiException apiException) {
      throw SamExceptionFactory.create("Error granting workspace role in Sam", apiException);
    }
  }

  /**
   * Wrapper around Sam client to remove a role from the provided user.
   *
   * <p>This operation is only available to MC_WORKSPACE stage workspaces, as Rawls manages
   * permissions directly on other workspaces. Trying to remove a role that a user does not have
   * will succeed, though Sam will error if the email is not a registered user.
   */
  @Traced
  public void removeWorkspaceRole(
      UUID workspaceId, AuthenticatedUserRequest userRequest, WsmIamRole role, String email)
      throws InterruptedException {
    stageService.assertMcWorkspace(workspaceId, "removeWorkspaceRole");
    checkAuthz(
        userRequest,
        SamConstants.SAM_WORKSPACE_RESOURCE,
        workspaceId.toString(),
        samActionToModifyRole(role));
    ResourcesApi resourceApi = samResourcesApi(userRequest.getRequiredToken());
    try {
      SamRetry.retry(
          () ->
              resourceApi.removeUserFromPolicy(
                  SamConstants.SAM_WORKSPACE_RESOURCE,
                  workspaceId.toString(),
                  role.toSamRole(),
                  email));
      logger.info(
          "Removed role {} from user {} in workspace {}", role.toSamRole(), email, workspaceId);
    } catch (ApiException apiException) {
      throw SamExceptionFactory.create("Error removing workspace role in Sam", apiException);
    }
  }

  /**
   * Wrapper around the Sam client to remove a role from the provided user on a controlled resource.
   *
   * <p>Similar to {@removeWorkspaceRole}, but for controlled resources. This should only be
   * necessary for private resources, as users do not have individual roles on shared resources.
   *
   * <p>This call to Sam is made as the WSM SA, as users do not have permission to directly modify
   * IAM on resources. This method still requires user credentials to validate as a safeguard, but
   * they are not used in the role removal call.
   *
   * @param resource The resource to remove a role from
   * @param userRequest User credentials. These are not used for the call to Sam, but must belong to
   *     a workspace owner to ensure the WSM SA is being used on a user's behalf correctly.
   * @param role The role to remove
   * @param email Email identifier of the user whose role is being removed.
   */
  @Traced
  public void removeResourceRole(
      ControlledResource resource,
      AuthenticatedUserRequest userRequest,
      ControlledResourceIamRole role,
      String email)
      throws InterruptedException {
    // Validate that the provided user credentials are an owner in the resource's workspace.
    // Although the Sam call to revoke a resource role must use WSM SA credentials instead, this
    // is a safeguard against accidentally invoking these credentials for unauthorized users.
    checkAuthz(
        userRequest,
        SamConstants.SAM_WORKSPACE_RESOURCE,
        resource.getWorkspaceId().toString(),
        SamConstants.SAM_WORKSPACE_OWN_ACTION);

    try {
      ResourcesApi wsmSaResourceApi = samResourcesApi(getWsmServiceAccountToken());
      SamRetry.retry(
          () ->
              wsmSaResourceApi.removeUserFromPolicyV2(
                  resource.getCategory().getSamResourceName(),
                  resource.getResourceId().toString(),
                  role.toSamRole(),
                  email));
      logger.info(
          "Removed role {} from user {} on resource {}",
          role.toSamRole(),
          email,
          resource.getResourceId());
    } catch (IOException credentialException) {
      throw new InternalServerErrorException(
          "Internal server error removing resource role in Sam", credentialException);
    } catch (ApiException apiException) {
      throw SamExceptionFactory.create("Sam error removing resource role in Sam", apiException);
    }
  }

  /**
   * Wrapper around the Sam client to restore a role to a user on a controlled resource. This is
   * only exposed to support undoing Stairway transactions which revoke access. It should not be
   * called otherwise.
   *
   * <p>This call to Sam is made as the WSM SA, as users do not have permission to directly modify
   * IAM on resources. This method still requires user credentials to validate as a safeguard, but
   * they are not used in the role removal call.
   *
   * @param resource The resource to restore a role to
   * @param userRequest User credentials. These are not used for the call to Sam, but must belong to
   *     a workspace owner to ensure the WSM SA is being used on a user's behalf correctly.
   * @param role The role to restore
   * @param email Email identifier of the user whose role is being restored.
   */
  @Traced
  public void restoreResourceRole(
      ControlledResource resource,
      AuthenticatedUserRequest userRequest,
      ControlledResourceIamRole role,
      String email)
      throws InterruptedException {
    // Validate that the provided user credentials are an owner in the resource's workspace.
    // Although the Sam call to revoke a resource role must use WSM SA credentials instead, this
    // is a safeguard against accidentally invoking these credentials for unauthorized users.
    checkAuthz(
        userRequest,
        SamConstants.SAM_WORKSPACE_RESOURCE,
        resource.getWorkspaceId().toString(),
        SamConstants.SAM_WORKSPACE_OWN_ACTION);

    try {
      ResourcesApi wsmSaResourceApi = samResourcesApi(getWsmServiceAccountToken());
      SamRetry.retry(
          () ->
              wsmSaResourceApi.addUserToPolicyV2(
                  resource.getCategory().getSamResourceName(),
                  resource.getResourceId().toString(),
                  role.toSamRole(),
                  email));
      logger.info(
          "Restored role {} to user {} on resource {}",
          role.toSamRole(),
          email,
          resource.getResourceId());
    } catch (IOException credentialException) {
      throw new InternalServerErrorException(
          "Internal server error restoring resource role in Sam", credentialException);
    } catch (ApiException apiException) {
      throw SamExceptionFactory.create("Sam error restoring resource role in Sam", apiException);
    }
  }

  /**
   * Wrapper around Sam client to retrieve the full current permissions model of a workspace.
   *
   * <p>This operation is only available to MC_WORKSPACE stage workspaces, as Rawls manages
   * permissions directly on other workspaces.
   */
  @Traced
  public List<RoleBinding> listRoleBindings(UUID workspaceId, AuthenticatedUserRequest userRequest)
      throws InterruptedException {
    stageService.assertMcWorkspace(workspaceId, "listRoleBindings");
    checkAuthz(
        userRequest,
        SamConstants.SAM_WORKSPACE_RESOURCE,
        workspaceId.toString(),
        SamConstants.SAM_WORKSPACE_READ_IAM_ACTION);
    ResourcesApi resourceApi = samResourcesApi(userRequest.getRequiredToken());
    try {
      List<AccessPolicyResponseEntry> samResult =
          SamRetry.retry(
              () ->
                  resourceApi.listResourcePolicies(
                      SamConstants.SAM_WORKSPACE_RESOURCE, workspaceId.toString()));
      return samResult.stream()
          .map(
              entry ->
                  RoleBinding.builder()
                      .role(WsmIamRole.fromSam(entry.getPolicyName()))
                      .users(entry.getPolicy().getMemberEmails())
                      .build())
          .collect(Collectors.toList());
    } catch (ApiException apiException) {
      throw SamExceptionFactory.create("Error listing role bindings in Sam", apiException);
    }
  }

  /** Wrapper around Sam client to fetch the list of users with a specific role in a workspace. */
  @Traced
  public List<String> listUsersWithWorkspaceRole(
      UUID workspaceId, WsmIamRole role, AuthenticatedUserRequest userRequest) {
    ResourcesApi resourcesApi = samResourcesApi(userRequest.getRequiredToken());
    try {
      return resourcesApi
          .getPolicyV2(
              SamConstants.SAM_WORKSPACE_RESOURCE, workspaceId.toString(), role.toSamRole())
          .getMemberEmails();
    } catch (ApiException e) {
      throw SamExceptionFactory.create("Error retrieving workspace policy members from Sam", e);
    }
  }

  /**
   * Wrapper around Sam client to sync a Sam policy to a Google group. Returns email of that group.
   *
   * <p>This operation in Sam is idempotent, so we don't worry about calling this multiple times.
   */
  @Traced
  public String syncWorkspacePolicy(
      UUID workspaceId, WsmIamRole role, AuthenticatedUserRequest userRequest)
      throws InterruptedException {
    String group =
        syncPolicyOnObject(
            SamConstants.SAM_WORKSPACE_RESOURCE,
            workspaceId.toString(),
            role.toSamRole(),
            userRequest);
    logger.info(
        "Synced role {} to google group {} in workspace {}", role.toSamRole(), group, workspaceId);
    return group;
  }

  /**
   * Wrapper around Sam client to sync a Sam policy on a controlled resource to a google group and
   * return the email of that group.
   *
   * <p>This should only be called for controlled resources which require permissions granted to
   * individual users, i.e. private or application-controlled resources. All other cases are handled
   * by the permissions that workspace-level roles inherit on resources via Sam's hierarchical
   * resources, and do not use the policies synced by this function.
   *
   * <p>This operation in Sam is idempotent, so we don't worry about calling this multiple times.
   *
   * @param resource The resource to sync a binding for
   * @param role The policy to sync in Sam
   * @param userRequest User authentication
   * @return
   */
  @Traced
  public String syncPrivateResourcePolicy(
      ControlledResource resource,
      ControlledResourceIamRole role,
      AuthenticatedUserRequest userRequest)
      throws InterruptedException {
    // TODO: in the future, this function will also be called for application managed resources,
    //  including app-shared. This check should be modified appropriately.
    if (resource.getAccessScope() != AccessScopeType.ACCESS_SCOPE_PRIVATE) {
      throw new InternalLogicException(
          "syncPrivateResourcePolicy should not be called for shared resources!");
    }
    String group =
        syncPolicyOnObject(
            resource.getCategory().getSamResourceName(),
            resource.getResourceId().toString(),
            role.toSamRole(),
            userRequest);
    logger.info(
        "Synced role {} to google group {} for resource {}",
        role.toSamRole(),
        group,
        resource.getResourceId());
    return group;
  }

  /**
   * Common implementation for syncing a policy to a Google group on an object in Sam.
   *
   * @param resourceTypeName The type of the Sam resource, as configured with Sam.
   * @param resourceId The Sam ID of the resource to sync a policy for
   * @param policyName The name of the policy to sync
   * @param userRequest User credentials to pass to Sam
   * @return The Google group whose membership is synced to the specified policy.
   */
  private String syncPolicyOnObject(
      String resourceTypeName,
      String resourceId,
      String policyName,
      AuthenticatedUserRequest userRequest)
      throws InterruptedException {
    GoogleApi googleApi = samGoogleApi(userRequest.getRequiredToken());
    try {
      // Sam makes no guarantees about what values are returned from the POST call, so we instead
      // fetch the group in a separate call after syncing.
      SamRetry.retry(() -> googleApi.syncPolicy(resourceTypeName, resourceId, policyName));
      return SamRetry.retry(() -> googleApi.syncStatus(resourceTypeName, resourceId, policyName))
          .getEmail();
    } catch (ApiException apiException) {
      throw SamExceptionFactory.create("Error syncing policy in Sam", apiException);
    }
  }

  /**
   * Create a controlled resource in Sam.
   *
   * @param resource The WSM representation of the resource to create.
   * @param privateIamRoles The IAM role(s) to grant a private user. Required for private resources,
   *     should be null otherwise.
   * @param userRequest Credentials to use for talking to Sam.
   */
  @Traced
  public void createControlledResource(
      ControlledResource resource,
      List<ControlledResourceIamRole> privateIamRoles,
      AuthenticatedUserRequest userRequest)
      throws InterruptedException {
    // Set up the master OWNER user in Sam for all controlled resources, if it's not already.
    initializeWsmServiceAccount();
    ResourcesApi resourceApi = samResourcesApi(userRequest.getRequiredToken());
    FullyQualifiedResourceId workspaceParentFqId =
        new FullyQualifiedResourceId()
            .resourceId(resource.getWorkspaceId().toString())
            .resourceTypeName(SamConstants.SAM_WORKSPACE_RESOURCE);
    CreateResourceRequestV2 resourceRequest =
        new CreateResourceRequestV2()
            .resourceId(resource.getResourceId().toString())
            .parent(workspaceParentFqId);

    addWsmResourceOwnerPolicy(resourceRequest);
    // Only create policies for private resources. Workspace role permissions are handled through
    // role-based inheritance in Sam instead. This should expand to include policies for
    // applications in the future.
    if (resource.getAccessScope() == AccessScopeType.ACCESS_SCOPE_PRIVATE) {
      // The assigned user is always the current user for private resources.
      addPrivateResourcePolicies(
          resourceRequest, privateIamRoles, getRequestUserEmail(userRequest));
    }

    try {
      SamRetry.retry(
          () ->
              resourceApi.createResourceV2(
                  resource.getCategory().getSamResourceName(), resourceRequest));
      logger.info("Created Sam controlled resource {}", resource.getResourceId());
    } catch (ApiException apiException) {
      // Do nothing if the resource to create already exists, this may not be the first time do is
      // called. Other exceptions still need to be surfaced.
      // Resource IDs are randomly generated, so we trust that the caller must have created
      // an existing Sam resource.
      logger.info(
          "Sam API error while creating a controlled resource, code is " + apiException.getCode());
      if (apiException.getCode() == HttpStatus.CONFLICT.value()) {
        logger.info(
            "Sam error was CONFLICT on creation request. This means the resource already "
                + "exists but is not an error so no exception thrown.");
        return;
      }
      throw SamExceptionFactory.create("Error creating controlled resource in Sam", apiException);
    }
  }

  @Traced
  public void deleteControlledResource(
      ControlledResource resource, AuthenticatedUserRequest userRequest)
      throws InterruptedException {
    ResourcesApi resourceApi = samResourcesApi(userRequest.getRequiredToken());
    try {
      SamRetry.retry(
          () ->
              resourceApi.deleteResourceV2(
                  resource.getCategory().getSamResourceName(),
                  resource.getResourceId().toString()));
      logger.info("Deleted Sam controlled resource {}", resource.getResourceId());
    } catch (ApiException apiException) {
      // Do nothing if the resource to delete is not found, this may not be the first time delete is
      // called. Other exceptions still need to be surfaced.
      logger.info(
          "Sam API error while deleting a controlled resource, code is " + apiException.getCode());
      if (apiException.getCode() == HttpStatus.NOT_FOUND.value()) {
        logger.info(
            "Sam error was NOT_FOUND on a deletion call. "
                + "This just means the deletion was tried twice so no error thrown.");
        return;
      }
      throw SamExceptionFactory.create("Error deleting controlled resource in Sam", apiException);
    }
  }

  /**
   * Return the list of roles a user has directly on a private, user-managed controlled resource.
   * This will not return roles that a user holds via group membership.
   *
   * <p>This call to Sam is made as the WSM SA, as users do not have permission to directly modify
   * IAM on resources. This method still requires user credentials to validate as a safeguard, but
   * they are not used in the role removal call.
   *
   * @param resource The resource to fetch roles on
   * @param userEmail Email identifier of the user whose role is being removed.
   * @param userRequest User credentials. These are not used for the call to Sam, but must belong to
   *     a workspace owner to ensure the WSM SA is being used on a user's behalf correctly.
   */
  public List<ControlledResourceIamRole> getUserRolesOnPrivateResource(
      ControlledResource resource, String userEmail, AuthenticatedUserRequest userRequest)
      throws InterruptedException {
    // Validate that the provided user credentials are an owner in the resource's workspace.
    // Although the Sam call to revoke a resource role must use WSM SA credentials instead, this
    // is a safeguard against accidentally invoking these credentials for unauthorized users.
    checkAuthz(
        userRequest,
        SamConstants.SAM_WORKSPACE_RESOURCE,
        resource.getWorkspaceId().toString(),
        SamConstants.SAM_WORKSPACE_OWN_ACTION);

    try {
      ResourcesApi wsmSaResourceApi = samResourcesApi(getWsmServiceAccountToken());
      List<AccessPolicyResponseEntryV2> policyList =
          wsmSaResourceApi.listResourcePoliciesV2(
              resource.getCategory().getSamResourceName(), resource.getResourceId().toString());
      return policyList.stream()
          .filter(policyEntry -> policyEntry.getPolicy().getMemberEmails().contains(userEmail))
          .map(AccessPolicyResponseEntryV2::getPolicyName)
          .map(ControlledResourceIamRole::fromSamRole)
          .collect(Collectors.toList());
    } catch (IOException credentialException) {
      throw new InternalServerErrorException(
          "Internal server error reading private resource roles from Sam", credentialException);
    } catch (ApiException apiException) {
      throw SamExceptionFactory.create("Sam error removing resource role in Sam", apiException);
    }
  }

  public Boolean status() {
    // No access token needed since this is an unauthenticated API.
    StatusApi statusApi = new StatusApi(getApiClient(null));
    try {
      SystemStatus samStatus = SamRetry.retry(statusApi::getSystemStatus);
      return samStatus.getOk();
    } catch (ApiException | InterruptedException e) {
      //  If any exception was thrown during the status check, return that the system is not OK.
      return false;
    }
  }

  /**
   * Builds a policy list with a single provided owner and empty reader, writer and application
   * policies.
   *
   * <p>This is a helper function for building the policy section of a request to create a workspace
   * resource in Sam. The provided user is granted the OWNER role and empty policies for reader,
   * writer, and application are also included.
   *
   * <p>The empty policies are included because Sam requires all policies on a workspace to be
   * provided at creation time. Although policy membership can be modified later, policy creation
   * must happen at the same time as workspace resource creation.
   */
  private Map<String, AccessPolicyMembershipV2> defaultWorkspacePolicies(String ownerEmail) {
    Map<String, AccessPolicyMembershipV2> policyMap = new HashMap<>();
    policyMap.put(
        WsmIamRole.OWNER.toSamRole(),
        new AccessPolicyMembershipV2()
            .addRolesItem(WsmIamRole.OWNER.toSamRole())
            .addMemberEmailsItem(ownerEmail));
    // For all non-owner roles, we create empty policies which can be modified later.
    for (WsmIamRole workspaceRole : WsmIamRole.values()) {
      if (workspaceRole != WsmIamRole.OWNER) {
        policyMap.put(
            workspaceRole.toSamRole(),
            new AccessPolicyMembershipV2().addRolesItem(workspaceRole.toSamRole()));
      }
    }
    return policyMap;
  }

  /**
   * Add WSM's service account as the owner of a controlled resource in Sam. Used for admin
   * reassignment of resources. This assumes samService.initialize() has already been called, which
   * should happen on start.
   */
  private void addWsmResourceOwnerPolicy(CreateResourceRequestV2 request)
      throws InterruptedException {
    try {
      String wsmSaEmail = getEmailFromToken(getWsmServiceAccountToken());
      AccessPolicyMembershipV2 ownerPolicy =
          new AccessPolicyMembershipV2()
              .addRolesItem(ControlledResourceIamRole.OWNER.toSamRole())
              .addMemberEmailsItem(wsmSaEmail);
      request.putPoliciesItem(ControlledResourceIamRole.OWNER.toSamRole(), ownerPolicy);
    } catch (IOException e) {
      // In cases where WSM is not running as a service account (e.g. unit tests), the above call to
      // get application default credentials will fail. This is fine, as those cases don't create
      // real resources.
      logger.warn(
          "Failed to add WSM service account as resource owner Sam. This is expected for tests.",
          e);
      return;
    }
  }

  /**
   * Add policies for managing private users via policy. All private resources will have reader,
   * writer, and editor policies even if they are empty to support potential later reassignment.
   * This method will likely expand to support policies for applications in the future.
   */
  private void addPrivateResourcePolicies(
      CreateResourceRequestV2 request,
      List<ControlledResourceIamRole> privateIamRoles,
      String privateUser) {
    AccessPolicyMembershipV2 readerPolicy =
        new AccessPolicyMembershipV2().addRolesItem(ControlledResourceIamRole.READER.toSamRole());
    AccessPolicyMembershipV2 writerPolicy =
        new AccessPolicyMembershipV2().addRolesItem(ControlledResourceIamRole.WRITER.toSamRole());
    AccessPolicyMembershipV2 editorPolicy =
        new AccessPolicyMembershipV2().addRolesItem(ControlledResourceIamRole.EDITOR.toSamRole());

    // Create a reader or writer role as specified by the user request, but also create empty
    // roles in case of later re-assignment.
    if (privateIamRoles.contains(ControlledResourceIamRole.WRITER)) {
      writerPolicy.addMemberEmailsItem(privateUser);
    } else if (privateIamRoles.contains(ControlledResourceIamRole.READER)) {
      readerPolicy.addMemberEmailsItem(privateUser);
    }

    if (privateIamRoles.contains(ControlledResourceIamRole.EDITOR)) {
      editorPolicy.addMemberEmailsItem(privateUser);
    }

    request.putPoliciesItem(ControlledResourceIamRole.READER.toSamRole(), readerPolicy);
    request.putPoliciesItem(ControlledResourceIamRole.WRITER.toSamRole(), writerPolicy);
    request.putPoliciesItem(ControlledResourceIamRole.EDITOR.toSamRole(), editorPolicy);
  }

  /** Fetch the email associated with an authToken from Sam. */
  private String getEmailFromToken(String authToken) throws InterruptedException {
    UsersApi usersApi = samUsersApi(authToken);
    try {
      return SamRetry.retry(() -> usersApi.getUserStatusInfo().getUserEmail());
    } catch (ApiException apiException) {
      throw SamExceptionFactory.create("Error getting user email from Sam", apiException);
    }
  }

  /**
   * Fetch the email of a user's pet service account in a given project. This request to Sam will
   * create the pet SA if it doesn't already exist.
   */
  public String getOrCreatePetSaEmail(String projectId, AuthenticatedUserRequest userRequest) {
    GoogleApi googleApi = samGoogleApi(userRequest.getRequiredToken());
    try {
      return googleApi.getPetServiceAccount(projectId);
    } catch (ApiException apiException) {
      throw SamExceptionFactory.create("Error getting pet service account from Sam", apiException);
    }
  }

  /** Returns the Sam action for modifying a given IAM role. */
  private String samActionToModifyRole(WsmIamRole role) {
    return String.format("share_policy::%s", role.toSamRole());
  }
}
