package bio.terra.workspace.service.workspace.flight;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import bio.terra.stairway.FlightDebugInfo;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.FlightState;
import bio.terra.stairway.FlightStatus;
import bio.terra.stairway.StepStatus;
import bio.terra.workspace.common.BaseConnectedTest;
import bio.terra.workspace.common.StairwayTestUtils;
import bio.terra.workspace.common.fixtures.ControlledResourceFixtures;
import bio.terra.workspace.connected.UserAccessUtils;
import bio.terra.workspace.connected.WorkspaceConnectedTestUtils;
import bio.terra.workspace.db.exception.WorkspaceNotFoundException;
import bio.terra.workspace.service.iam.AuthenticatedUserRequest;
import bio.terra.workspace.service.job.JobMapKeys;
import bio.terra.workspace.service.job.JobService;
import bio.terra.workspace.service.resource.controlled.ControlledBigQueryDatasetResource;
import bio.terra.workspace.service.resource.controlled.ControlledResource;
import bio.terra.workspace.service.resource.controlled.ControlledResourceService;
import bio.terra.workspace.service.workspace.WorkspaceService;
import bio.terra.workspace.service.workspace.model.Workspace;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;

public class WorkspaceDeleteFlightTest extends BaseConnectedTest {

  /**
   * How long to wait for a delete context Stairway flight to complete before timing out the test.
   */
  private static final Duration DELETION_FLIGHT_TIMEOUT = Duration.ofMinutes(3);

  @Autowired UserAccessUtils userAccessUtils;
  @Autowired WorkspaceConnectedTestUtils connectedTestUtils;
  @Autowired ControlledResourceService controlledResourceService;
  @Autowired JobService jobService;
  @Autowired WorkspaceService workspaceService;

  @Test
  @DisabledIfEnvironmentVariable(named = "TEST_ENV", matches = BUFFER_SERVICE_DISABLED_ENVS_REG_EX)
  void deleteMcWorkspaceWithResource() throws Exception {
    // Create a workspace with a controlled resource
    AuthenticatedUserRequest userRequest = userAccessUtils.defaultUserAuthRequest();
    Workspace workspace = connectedTestUtils.createWorkspaceWithGcpContext(userRequest);
    ControlledBigQueryDatasetResource dataset =
        ControlledResourceFixtures.makeDefaultControlledBigQueryDatasetResource()
            .workspaceId(workspace.getWorkspaceId())
            .build();
    var creationParameters =
        ControlledResourceFixtures.defaultBigQueryDatasetCreationParameters()
            .datasetId(dataset.getDatasetName());
    controlledResourceService.createBigQueryDataset(
        dataset, creationParameters, Collections.emptyList(), userRequest);

    ControlledResource gotResource =
        controlledResourceService.getControlledResource(
            workspace.getWorkspaceId(), dataset.getResourceId(), userRequest);
    assertEquals(dataset, gotResource.castToBigQueryDatasetResource());

    // Run the delete flight, retrying every step once
    FlightMap deleteParameters = new FlightMap();
    deleteParameters.put(
        WorkspaceFlightMapKeys.WORKSPACE_ID, workspace.getWorkspaceId().toString());
    deleteParameters.put(
        WorkspaceFlightMapKeys.WORKSPACE_STAGE, workspace.getWorkspaceStage().name());
    deleteParameters.put(JobMapKeys.AUTH_USER_INFO.getKeyName(), userRequest);

    Map<String, StepStatus> doFailures = new HashMap<>();
    doFailures.put(
        DeleteControlledSamResourcesStep.class.getName(), StepStatus.STEP_RESULT_FAILURE_RETRY);
    doFailures.put(DeleteProjectStep.class.getName(), StepStatus.STEP_RESULT_FAILURE_RETRY);
    doFailures.put(DeleteWorkspaceAuthzStep.class.getName(), StepStatus.STEP_RESULT_FAILURE_RETRY);
    doFailures.put(DeleteWorkspaceStateStep.class.getName(), StepStatus.STEP_RESULT_FAILURE_RETRY);
    FlightDebugInfo debugInfo = FlightDebugInfo.newBuilder().doStepFailures(doFailures).build();

    FlightState flightState =
        StairwayTestUtils.blockUntilFlightCompletes(
            jobService.getStairway(),
            WorkspaceDeleteFlight.class,
            deleteParameters,
            DELETION_FLIGHT_TIMEOUT,
            debugInfo);
    assertEquals(FlightStatus.SUCCESS, flightState.getFlightStatus());

    // Verify the resource and workspace are not in WSM DB
    assertThrows(
        WorkspaceNotFoundException.class,
        () ->
            controlledResourceService.getControlledResource(
                dataset.getWorkspaceId(), dataset.getResourceId(), userRequest));
    assertThrows(
        WorkspaceNotFoundException.class,
        () -> workspaceService.getWorkspace(workspace.getWorkspaceId(), userRequest));
  }

  @Test
  @DisabledIfEnvironmentVariable(named = "TEST_ENV", matches = BUFFER_SERVICE_DISABLED_ENVS_REG_EX)
  void cannotUndoWorkspaceDelete() throws Exception {
    // Create a workspace with a controlled resource
    AuthenticatedUserRequest userRequest = userAccessUtils.defaultUserAuthRequest();
    Workspace workspace = connectedTestUtils.createWorkspaceWithGcpContext(userRequest);
    ControlledBigQueryDatasetResource dataset =
        ControlledResourceFixtures.makeDefaultControlledBigQueryDatasetResource()
            .workspaceId(workspace.getWorkspaceId())
            .build();
    var creationParameters =
        ControlledResourceFixtures.defaultBigQueryDatasetCreationParameters()
            .datasetId(dataset.getDatasetName());
    controlledResourceService.createBigQueryDataset(
        dataset, creationParameters, Collections.emptyList(), userRequest);

    ControlledResource gotResource =
        controlledResourceService.getControlledResource(
            workspace.getWorkspaceId(), dataset.getResourceId(), userRequest);
    assertEquals(dataset, gotResource.castToBigQueryDatasetResource());

    FlightMap deleteParameters = new FlightMap();
    deleteParameters.put(
        WorkspaceFlightMapKeys.WORKSPACE_ID, workspace.getWorkspaceId().toString());
    deleteParameters.put(
        WorkspaceFlightMapKeys.WORKSPACE_STAGE, workspace.getWorkspaceStage().name());
    deleteParameters.put(JobMapKeys.AUTH_USER_INFO.getKeyName(), userRequest);

    // Deletion steps can't be undone, so if the flight fails at the end, this should be marked as
    // a dismal failure and the deletion should persist.
    FlightDebugInfo debugInfo = FlightDebugInfo.newBuilder().lastStepFailure(true).build();
    FlightState flightState =
        StairwayTestUtils.blockUntilFlightCompletes(
            jobService.getStairway(),
            WorkspaceDeleteFlight.class,
            deleteParameters,
            DELETION_FLIGHT_TIMEOUT,
            debugInfo);
    assertEquals(FlightStatus.FATAL, flightState.getFlightStatus());

    // Verify the resource and workspace are still deleted, as delete steps have no undo.
    assertThrows(
        WorkspaceNotFoundException.class,
        () ->
            controlledResourceService.getControlledResource(
                dataset.getWorkspaceId(), dataset.getResourceId(), userRequest));
    assertThrows(
        WorkspaceNotFoundException.class,
        () -> workspaceService.getWorkspace(workspace.getWorkspaceId(), userRequest));
  }
}
