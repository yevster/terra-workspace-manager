package bio.terra.workspace.service.workspace.flight;

public final class WorkspaceFlightMapKeys {
  public static final String WORKSPACE_ID = "workspaceId";
  public static final String SPEND_PROFILE_ID = "spendProfileId";
  public static final String GCP_PROJECT_ID = "gcpProjectId";
  public static final String WORKSPACE_STAGE = "workspaceStage";
  public static final String BILLING_ACCOUNT_ID = "billingAccountId";
  public static final String IAM_GROUP_EMAIL_MAP = "iamGroupEmailMap";
  public static final String RBS_RESOURCE_ID = "rbsResourceId";
  public static final String DISPLAY_NAME = "displayNameId";
  public static final String DESCRIPTION = "descriptionId";
  public static final String USER_TO_REMOVE = "userToRemove";
  public static final String ROLE_TO_REMOVE = "roleToRemove";

  private WorkspaceFlightMapKeys() {}

  /** Use inner class for new set of keys so it's easy to spot duplicates */
  public static class ControlledResourceKeys {

    private ControlledResourceKeys() {}

    public static final String CREATION_PARAMETERS = "creationParameters";
    public static final String PRIVATE_RESOURCE_IAM_ROLES = "iamRoles";
    public static final String IAM_RESOURCE_GROUP_EMAIL_MAP = "iamResourceGroupEmailMap";

    public static final String UPDATE_PARAMETERS = "updateParameters";
    public static final String PREVIOUS_UPDATE_PARAMETERS = "previousUpdateParameters";

    public static final String RESOURCE_NAME = "resourceName";
    public static final String PREVIOUS_RESOURCE_NAME = "previousResourceName";
    public static final String RESOURCE_DESCRIPTION = "resourceDescription";
    public static final String PREVIOUS_RESOURCE_DESCRIPTION = "previousResourceDescription";

    public static final String RESOURCE_ROLES_TO_REMOVE = "resourceRolesToRemove";
    public static final String REMOVED_USER_IS_WORKSPACE_MEMBER = "removedUserIsWorkspaceMember";

    // Notebooks keys
    public static final String CREATE_NOTEBOOK_NETWORK_NAME = "createNotebookNetworkName";
    public static final String CREATE_NOTEBOOK_PARAMETERS = "createNotebookParameters";
    public static final String CREATE_NOTEBOOK_REGION = "createNotebookRegion";
    public static final String CREATE_NOTEBOOK_SERVICE_ACCOUNT_ID =
        "createNotebookServiceAccountId";
    public static final String CREATE_NOTEBOOK_SUBNETWORK_NAME = "createNotebookSubnetworkName";
    public static final String DELETE_NOTEBOOK_SERVICE_ACCOUNT_EMAIL =
        "deleteNotebookServiceAccountEmail";

    // Cloning Keys
    public static final String CLONE_ALL_RESOURCES_FLIGHT_ID = "cloneAllResourcesFlightId";
    public static final String CLONE_DEFINITION_RESULT = "cloneDefinitionResult";
    public static final String CLONED_RESOURCE_DEFINITION = "clonedResourceDefinition";
    public static final String CLONING_INSTRUCTIONS = "cloningInstructions";
    public static final String CONTROL_PLANE_PROJECT_ID = "controlPlaneProjectId";
    public static final String CREATE_CLOUD_CONTEXT_FLIGHT_ID = "createCloudContextFlightId";
    public static final String DESTINATION_BUCKET_NAME = "destinationBucketName";
    public static final String DESTINATION_CLONE_INPUTS = "destinationCloneInputs";
    public static final String DESTINATION_DATASET_NAME = "destinationDatasetName";
    public static final String DESTINATION_REFERENCED_RESOURCE = "destinationReferencedResource";
    public static final String DESTINATION_WORKSPACE_ID = "destinationWorkspaceId";
    public static final String LOCATION = "location";
    public static final String RESOURCE_ID_TO_CLONE_RESULT = "resourceIdToCloneResult";
    public static final String RESOURCES_TO_CLONE = "resourcesToClone";
    public static final String SOURCE_CLONE_INPUTS = "sourceCloneInputs";
    public static final String SOURCE_WORKSPACE_ID = "sourceWorkspaceId";
    public static final String STORAGE_TRANSFER_JOB_NAME = "storageTransferJobName";
    public static final String STORAGE_TRANSFER_SERVICE_SA_EMAIL = "storageTransferServiceSAEmail";
    public static final String TABLE_TO_JOB_ID_MAP = "tableToJobIdMap";
    public static final String WORKSPACE_CREATE_FLIGHT_ID = "workspaceCreateFlightId";
  }

  /** Common resource keys */
  public static class ResourceKeys {
    public static final String RESOURCE_ID = "resourceId";
    public static final String RESOURCE_TYPE = "resourceType";

    private ResourceKeys() {}
  }
}
