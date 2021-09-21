package bio.terra.workspace.service.workspace.flight.create.azure;

import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.workspace.db.WorkspaceDao;
import bio.terra.workspace.service.job.JobMapKeys;
import bio.terra.workspace.service.workspace.model.AzureCloudContext;
import java.util.UUID;

/**
 * Stores the previously generated Google Project Id in the {@link WorkspaceDao} as the Google cloud
 * context for the workspace.
 */
public class StoreAzureContextStep implements Step {
  private final WorkspaceDao workspaceDao;
  private final UUID workspaceId;

  public StoreAzureContextStep(WorkspaceDao workspaceDao, UUID workspaceId) {
    this.workspaceDao = workspaceDao;
    this.workspaceId = workspaceId;
  }

  @Override
  public StepResult doStep(FlightContext flightContext) throws InterruptedException {
    AzureCloudContext azureCloudContext =
        flightContext
            .getInputParameters()
            .get(JobMapKeys.REQUEST.getKeyName(), AzureCloudContext.class);
    // Create the cloud context; throws if the context already exists.
    workspaceDao.createAzureCloudContext(
        workspaceId, azureCloudContext, flightContext.getFlightId());
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext flightContext) throws InterruptedException {
    AzureCloudContext azureCloudContext =
        flightContext
            .getInputParameters()
            .get(JobMapKeys.REQUEST.getKeyName(), AzureCloudContext.class);

    // Delete the cloud context, but only if it is the one with our project id
    workspaceDao.deleteAzureCloudContextWithCheck(workspaceId, flightContext.getFlightId());
    return StepResult.getStepResultSuccess();
  }
}
