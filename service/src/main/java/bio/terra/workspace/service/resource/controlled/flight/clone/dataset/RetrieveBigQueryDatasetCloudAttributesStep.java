package bio.terra.workspace.service.resource.controlled.flight.clone.dataset;

import bio.terra.cloudres.google.bigquery.BigQueryCow;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import bio.terra.stairway.exception.RetryException;
import bio.terra.workspace.service.crl.CrlService;
import bio.terra.workspace.service.iam.AuthenticatedUserRequest;
import bio.terra.workspace.service.resource.controlled.ControlledBigQueryDatasetResource;
import bio.terra.workspace.service.workspace.WorkspaceService;
import bio.terra.workspace.service.workspace.flight.WorkspaceFlightMapKeys.ControlledResourceKeys;
import com.google.api.client.util.Strings;
import com.google.api.services.bigquery.model.Dataset;
import java.io.IOException;

/** Retrieve the Dataset creation parameters from the cloud object. */
public class RetrieveBigQueryDatasetCloudAttributesStep implements Step {

  private final ControlledBigQueryDatasetResource datasetResource;
  private final CrlService crlService;
  private final WorkspaceService workspaceService;
  private final AuthenticatedUserRequest userRequest;

  public RetrieveBigQueryDatasetCloudAttributesStep(
      ControlledBigQueryDatasetResource datasetResource,
      CrlService crlService,
      WorkspaceService workspaceService,
      AuthenticatedUserRequest userRequest) {
    this.datasetResource = datasetResource;
    this.crlService = crlService;
    this.workspaceService = workspaceService;
    this.userRequest = userRequest;
  }

  @Override
  public StepResult doStep(FlightContext flightContext)
      throws InterruptedException, RetryException {
    final String suppliedLocation =
        flightContext.getInputParameters().get(ControlledResourceKeys.LOCATION, String.class);
    if (!Strings.isNullOrEmpty(suppliedLocation)) {
      flightContext.getWorkingMap().put(ControlledResourceKeys.LOCATION, suppliedLocation);
      // we can stop here as we don't need the original location
      return StepResult.getStepResultSuccess();
    }
    // Since no location was specified, we need to find the original one
    // from the source dataset.
    final String projectId =
        workspaceService.getRequiredGcpProject(datasetResource.getWorkspaceId());
    final BigQueryCow bigQueryCow = crlService.createBigQueryCow(userRequest);
    try {
      final Dataset dataset =
          bigQueryCow.datasets().get(projectId, datasetResource.getDatasetName()).execute();
      final String sourceLocation = dataset.getLocation();
      flightContext.getWorkingMap().put(ControlledResourceKeys.LOCATION, sourceLocation);
      return StepResult.getStepResultSuccess();
    } catch (IOException e) {
      // TODO: consider retry here
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, e);
    }
  }

  // No side effects to undo
  @Override
  public StepResult undoStep(FlightContext flightContext) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }
}
