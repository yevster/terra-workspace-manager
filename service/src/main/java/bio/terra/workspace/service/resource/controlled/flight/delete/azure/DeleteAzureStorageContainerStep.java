package bio.terra.workspace.service.resource.controlled.flight.delete.azure;

import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import bio.terra.workspace.db.ResourceDao;
import bio.terra.workspace.service.resource.WsmResource;
import bio.terra.workspace.service.resource.controlled.azure.ControlledAzureStorageContainerResource;
import bio.terra.workspace.service.workspace.WorkspaceService;
import bio.terra.workspace.service.workspace.exceptions.AzureContextRequiredException;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A step for deleting a controlled GCS bucket resource. This step uses the following process to
 * actually delete the bucket: a. Set the lifecycle on the bucket to delete immediately b. Try
 * deleting the bucket c. If delete succeeds, finish step d. If delete fails, sleep one hour; goto
 * (either a or b; maybe a for belts and suspenders)
 *
 * <p>As this may take hours to days to complete, this step should never run as part of a
 * synchronous flight.
 */
// TODO: when Stairway implements timed waits, we can use those and not sit on a thread sleeping
//  for three days.
public class DeleteAzureStorageContainerStep implements Step {
  private static final int MAX_DELETE_TRIES = 72; // 3 days
  private final ResourceDao resourceDao;
  private final WorkspaceService workspaceService;
  private final UUID workspaceId;
  private final UUID resourceId;

  private final Logger logger = LoggerFactory.getLogger(DeleteAzureStorageContainerStep.class);

  public DeleteAzureStorageContainerStep(
      ResourceDao resourceDao,
      WorkspaceService workspaceService,
      UUID workspaceId,
      UUID resourceId) {
    this.resourceDao = resourceDao;
    this.workspaceService = workspaceService;
    this.workspaceId = workspaceId;
    this.resourceId = resourceId;
  }

  @Override
  public StepResult doStep(FlightContext flightContext) throws InterruptedException {
    int deleteTries = 0;
    WsmResource wsmResource = resourceDao.getResource(workspaceId, resourceId);
    ControlledAzureStorageContainerResource resource =
        ControlledAzureStorageContainerResource.cast(wsmResource.castToControlledResource());
    final var azureContext =
        workspaceService
            .getAzureCloudContext(resource.getWorkspaceId())
            .orElseThrow(() -> new AzureContextRequiredException("Azure not configured"));
    final var storageManager = azureContext.getAzureStorageManager();

    /// Check for existing blobs
    final var storageAccount =
        storageManager
            .storageAccounts()
            .getByResourceGroup(
                azureContext.getAzureResourceGroupId(), resource.getStorageAccountName());
    final var blobClient =
        azureContext
            .authenticatedBlobContainerClientBuilder()
            .containerName(resource.getContainerName())
            .endpoint(storageAccount.endPoints().primary().blob())
            .buildClient();

    final var existingBlobs = blobClient.listBlobs();
    if (existingBlobs.iterator().hasNext()) {
      return new StepResult(
          StepStatus.STEP_RESULT_FAILURE_FATAL,
          new Exception("Blob Container " + resource.getContainerName() + " is not empty."));
    }

    // Delete the container by name
    final var containerClient = storageManager.serviceClient().getBlobContainers();

    containerClient.delete(
        azureContext.getAzureResourceGroupId(),
        resource.getStorageAccountName(),
        resource.getContainerName());
    // Failure of the above should be wrapped in a Runtime Exception and thrown at this point.
    // Now, if no containers remain, delete the storage account.
    var remainingContainers =
        containerClient.list(
            azureContext.getAzureResourceGroupId(), resource.getStorageAccountName());
    if (!remainingContainers.iterator().hasNext()) {
      storageManager.storageAccounts().deleteById(storageAccount.id());
    }
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext flightContext) throws InterruptedException {
    logger.error(
        "Cannot undo delete of Azure Storage Container resource {} in workspace {}.",
        resourceId,
        workspaceId);
    // Surface whatever error caused Stairway to begin undoing.
    return flightContext.getResult();
  }
}
