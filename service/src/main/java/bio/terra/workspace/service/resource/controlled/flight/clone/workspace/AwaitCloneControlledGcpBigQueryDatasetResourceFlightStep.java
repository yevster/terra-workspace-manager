package bio.terra.workspace.service.resource.controlled.flight.clone.workspace;

import static bio.terra.workspace.common.utils.FlightUtils.FLIGHT_POLL_CYCLES;
import static bio.terra.workspace.common.utils.FlightUtils.FLIGHT_POLL_SECONDS;
import static bio.terra.workspace.common.utils.FlightUtils.validateRequiredEntries;

import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.FlightState;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import bio.terra.stairway.exception.DatabaseOperationException;
import bio.terra.stairway.exception.FlightWaitTimedOutException;
import bio.terra.stairway.exception.RetryException;
import bio.terra.workspace.common.utils.FlightUtils;
import bio.terra.workspace.generated.model.ApiClonedControlledGcpBigQueryDataset;
import bio.terra.workspace.generated.model.ApiGcpBigQueryDatasetResource;
import bio.terra.workspace.generated.model.ApiResourceMetadata;
import bio.terra.workspace.service.job.JobMapKeys;
import bio.terra.workspace.service.resource.WsmResourceType;
import bio.terra.workspace.service.resource.controlled.ControlledBigQueryDatasetResource;
import bio.terra.workspace.service.resource.model.StewardshipType;
import bio.terra.workspace.service.workspace.flight.WorkspaceFlightMapKeys.ControlledResourceKeys;
import bio.terra.workspace.service.workspace.model.WsmCloneResourceResult;
import bio.terra.workspace.service.workspace.model.WsmResourceCloneDetails;
import com.fasterxml.jackson.core.type.TypeReference;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Wait for the clone BigQuery dataset flight to complete and add the result to the appropriate map
 */
public class AwaitCloneControlledGcpBigQueryDatasetResourceFlightStep implements Step {

  private final ControlledBigQueryDatasetResource resource;
  private final String subflightId;

  public AwaitCloneControlledGcpBigQueryDatasetResourceFlightStep(
      ControlledBigQueryDatasetResource resource, String subflightId) {
    this.resource = resource;
    this.subflightId = subflightId;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    // wait for the flight
    try {
      final FlightState subflightState =
          context.getStairway().waitForFlight(subflightId, FLIGHT_POLL_SECONDS, FLIGHT_POLL_CYCLES);
      final WsmResourceCloneDetails cloneDetails = new WsmResourceCloneDetails();
      final WsmCloneResourceResult cloneResult =
          WorkspaceCloneUtils.flightStatusToCloneResult(subflightState.getFlightStatus(), resource);
      cloneDetails.setResult(cloneResult);

      final FlightMap resultMap = FlightUtils.getResultMapRequired(subflightState);
      final var clonedDataset =
          resultMap.get(
              JobMapKeys.RESPONSE.getKeyName(), ApiClonedControlledGcpBigQueryDataset.class);
      cloneDetails.setStewardshipType(StewardshipType.CONTROLLED);
      cloneDetails.setResourceType(WsmResourceType.BIG_QUERY_DATASET);
      cloneDetails.setCloningInstructions(resource.getCloningInstructions());
      cloneDetails.setSourceResourceId(resource.getResourceId());
      cloneDetails.setDestinationResourceId(
          Optional.ofNullable(clonedDataset)
              .map(ApiClonedControlledGcpBigQueryDataset::getDataset)
              .map(ApiGcpBigQueryDatasetResource::getMetadata)
              .map(ApiResourceMetadata::getResourceId)
              .orElse(null));
      String errorMessage = FlightUtils.getFlightErrorMessage(subflightState);
      cloneDetails.setErrorMessage(errorMessage);

      cloneDetails.setName(resource.getName());
      cloneDetails.setDescription(resource.getDescription());
      // add to the map
      final var resourceIdToResult =
          Optional.ofNullable(
                  context
                      .getWorkingMap()
                      .get(
                          ControlledResourceKeys.RESOURCE_ID_TO_CLONE_RESULT,
                          new TypeReference<Map<UUID, WsmResourceCloneDetails>>() {}))
              .orElseGet(HashMap::new);
      resourceIdToResult.put(resource.getResourceId(), cloneDetails);
      context
          .getWorkingMap()
          .put(ControlledResourceKeys.RESOURCE_ID_TO_CLONE_RESULT, resourceIdToResult);

    } catch (DatabaseOperationException | FlightWaitTimedOutException e) {
      // Retry for database issues or expired wait loop
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, e);
    }
    validateRequiredEntries(
        context.getWorkingMap(), ControlledResourceKeys.RESOURCE_ID_TO_CLONE_RESULT);
    return StepResult.getStepResultSuccess();
  }

  // Nothing to undo
  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }
}
