package bio.terra.workspace.service.resource.controlled.flight.clone.workspace;

import static bio.terra.workspace.common.utils.FlightUtils.validateRequiredEntries;

import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import bio.terra.workspace.service.workspace.flight.WorkspaceFlightMapKeys.ControlledResourceKeys;
import java.util.UUID;

/**
 * Generate flight IDs and a workspace ID or two ahead of the steps that need them. The steps can
 * then check the existence of the flights or workspaces before creation to ensure idempotency.
 */
public class CreateIdsForFutureStepsStep implements Step {

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    final FlightMap workingMap = context.getWorkingMap();
    workingMap.put(
        ControlledResourceKeys.CREATE_CLOUD_CONTEXT_FLIGHT_ID,
        context.getStairway().createFlightId());
    workingMap.put(ControlledResourceKeys.DESTINATION_WORKSPACE_ID, UUID.randomUUID());
    workingMap.put(
        ControlledResourceKeys.WORKSPACE_CREATE_FLIGHT_ID, context.getStairway().createFlightId());
    workingMap.put(
        ControlledResourceKeys.CLONE_ALL_RESOURCES_FLIGHT_ID,
        context.getStairway().createFlightId());
    validateRequiredEntries(
        workingMap,
        ControlledResourceKeys.CREATE_CLOUD_CONTEXT_FLIGHT_ID,
        ControlledResourceKeys.DESTINATION_WORKSPACE_ID,
        ControlledResourceKeys.WORKSPACE_CREATE_FLIGHT_ID,
        ControlledResourceKeys.CLONE_ALL_RESOURCES_FLIGHT_ID);
    return StepResult.getStepResultSuccess();
  }

  // No side effects to undo.
  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }
}
