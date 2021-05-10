package bio.terra.workspace.service.workspace.flight;

import bio.terra.cloudres.google.billing.CloudBillingClientCow;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import com.google.cloud.billing.v1.ProjectBillingInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A {@link Step} to set the billing account on the Google project. */
public class SetProjectBillingStep implements Step {
  private final Logger logger = LoggerFactory.getLogger(SetProjectBillingStep.class);

  private final CloudBillingClientCow billingClient;

  public SetProjectBillingStep(CloudBillingClientCow billingClient) {
    this.billingClient = billingClient;
  }

  @Override
  public StepResult doStep(FlightContext flightContext) {
    logger.info("Start SetProjectBillingStep: ");

    String projectId = "terra-wsm-test-51f704f6";
    String billingAccountId = "billingAccounts/01A82E-CA8A14-367457";
    logger.info("Start SetProjectBillingStep22222222: ");
    ProjectBillingInfo setBilling =
        ProjectBillingInfo.newBuilder()
            .setBillingAccountName("billingAccounts/" + billingAccountId)
            .build();
    logger.info("Start SetProjectBillingStep333333333: ");
    billingClient.updateProjectBillingInfo("projects/" + projectId, setBilling);
    logger.info("Start SetProjectBillingStep44444444: ");
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext flightContext) {
    // We're going to delete the project, so we don't need to worry about removing the billing
    // account.
    return StepResult.getStepResultSuccess();
  }
}
