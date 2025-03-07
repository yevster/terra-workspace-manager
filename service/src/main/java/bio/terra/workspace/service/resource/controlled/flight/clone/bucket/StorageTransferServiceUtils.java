package bio.terra.workspace.service.resource.controlled.flight.clone.bucket;

import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import bio.terra.workspace.service.workspace.flight.WorkspaceFlightMapKeys.ControlledResourceKeys;
import com.google.api.client.googleapis.util.Utils;
import com.google.api.services.storagetransfer.v1.Storagetransfer;
import com.google.api.services.storagetransfer.v1.StoragetransferScopes;
import com.google.api.services.storagetransfer.v1.model.TransferJob;
import com.google.api.services.storagetransfer.v1.model.UpdateTransferJobRequest;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class StorageTransferServiceUtils {
  private static final Logger logger = LoggerFactory.getLogger(StorageTransferServiceUtils.class);
  private static final String APPLICATION_NAME = "terra-workspace-manager";
  private static final String DELETED_STATUS = "DELETED";

  private StorageTransferServiceUtils() {}

  public static Storagetransfer createStorageTransferService() throws IOException {
    GoogleCredentials credential = GoogleCredentials.getApplicationDefault();
    if (credential.createScopedRequired()) {
      credential = credential.createScoped(StoragetransferScopes.all());
    }

    return new Storagetransfer.Builder(
            Utils.getDefaultTransport(),
            Utils.getDefaultJsonFactory(),
            new HttpCredentialsAdapter(credential))
        .setApplicationName(APPLICATION_NAME)
        .build();
  }

  /**
   * Delete the transfer job, as we don't support reusing them.
   *
   * @param storageTransferService - transfer service
   * @param transferJobName - unidque name of the transfer job
   * @param controlPlaneProjectId - GCP project ID of the control plane
   * @throws IOException
   */
  public static void deleteTransferJob(
      Storagetransfer storageTransferService, String transferJobName, String controlPlaneProjectId)
      throws IOException {
    final TransferJob patchedTransferJob = new TransferJob().setStatus(DELETED_STATUS);
    final UpdateTransferJobRequest updateTransferJobRequest =
        new UpdateTransferJobRequest()
            .setUpdateTransferJobFieldMask("status")
            .setTransferJob(patchedTransferJob)
            .setProjectId(controlPlaneProjectId);
    final TransferJob deletedTransferJob =
        storageTransferService
            .transferJobs()
            .patch(transferJobName, updateTransferJobRequest)
            .execute();
    if (!DELETED_STATUS.equals(deletedTransferJob.getStatus())) {
      logger.warn("Failed to delete transfer job {}", deletedTransferJob.getName());
    }
  }

  /**
   * A reusable step implementation for deleting a storage transfer job.
   *
   * @param flightContext
   * @return
   */
  public static StepResult deleteTransferJobStepImpl(FlightContext flightContext) {
    try {
      final Storagetransfer storageTransferService = createStorageTransferService();
      final String transferJobName =
          createTransferJobName(flightContext.getFlightId()); // might not be in map yet
      final String controlPlaneProjectId =
          flightContext
              .getWorkingMap()
              .get(ControlledResourceKeys.CONTROL_PLANE_PROJECT_ID, String.class);
      deleteTransferJob(storageTransferService, transferJobName, controlPlaneProjectId);
    } catch (IOException e) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, e);
    }
    return StepResult.getStepResultSuccess();
  }

  /**
   * Construct the name to use for the transfer job, which must be globally unique. Use the flight
   * ID for the job name so we can find it after a restart.
   *
   * @param flightId - random ID for this flight
   * @return - the job name
   */
  public static String createTransferJobName(String flightId) {
    return "transferJobs/wsm-" + flightId;
  }
}
