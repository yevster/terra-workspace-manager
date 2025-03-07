package bio.terra.workspace.service.workspace.flight;

import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import bio.terra.workspace.common.utils.FlightUtils;
import bio.terra.workspace.db.WorkspaceDao;
import bio.terra.workspace.service.spendprofile.SpendProfileId;
import bio.terra.workspace.service.workspace.model.Workspace;
import bio.terra.workspace.service.workspace.model.WorkspaceStage;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;

public class CreateWorkspaceStep implements Step {

  private final WorkspaceDao workspaceDao;

  private final Logger logger = LoggerFactory.getLogger(CreateWorkspaceStep.class);

  public CreateWorkspaceStep(WorkspaceDao workspaceDao) {
    this.workspaceDao = workspaceDao;
  }

  @Override
  public StepResult doStep(FlightContext flightContext)
      throws RetryException, InterruptedException {
    FlightMap inputMap = flightContext.getInputParameters();

    UUID workspaceId =
        UUID.fromString(inputMap.get(WorkspaceFlightMapKeys.WORKSPACE_ID, String.class));

    String spendProfileIdString =
        inputMap.get(WorkspaceFlightMapKeys.SPEND_PROFILE_ID, String.class);
    SpendProfileId spendProfileId =
        Optional.ofNullable(spendProfileIdString).map(SpendProfileId::create).orElse(null);

    String displayName = inputMap.get(WorkspaceFlightMapKeys.DISPLAY_NAME, String.class);
    String description = inputMap.get(WorkspaceFlightMapKeys.DESCRIPTION, String.class);

    WorkspaceStage workspaceStage =
        WorkspaceStage.valueOf(inputMap.get(WorkspaceFlightMapKeys.WORKSPACE_STAGE, String.class));
    Workspace workspaceToCreate =
        Workspace.builder()
            .workspaceId(workspaceId)
            .spendProfileId(spendProfileId)
            .workspaceStage(workspaceStage)
            .displayName(displayName)
            .description(description)
            .build();

    workspaceDao.createWorkspace(workspaceToCreate);

    FlightUtils.setResponse(flightContext, workspaceId, HttpStatus.OK);
    logger.info("Workspace created with id {}", workspaceId);

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext flightContext) throws InterruptedException {
    FlightMap inputMap = flightContext.getInputParameters();
    UUID workspaceId =
        UUID.fromString(inputMap.get(WorkspaceFlightMapKeys.WORKSPACE_ID, String.class));
    // Ignore return value, as we don't care whether a workspace was deleted or just not found.
    workspaceDao.deleteWorkspace(workspaceId);
    return StepResult.getStepResultSuccess();
  }
}
