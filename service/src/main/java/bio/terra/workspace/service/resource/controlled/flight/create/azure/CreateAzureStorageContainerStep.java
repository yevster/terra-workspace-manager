package bio.terra.workspace.service.resource.controlled.flight.create.azure;

import bio.terra.stairway.*;
import bio.terra.stairway.exception.RetryException;
import bio.terra.workspace.generated.model.ApiAzureStorageContainerCreationParameters;
import bio.terra.workspace.service.resource.controlled.AccessScopeType;
import bio.terra.workspace.service.resource.controlled.azure.ControlledAzureStorageContainerResource;
import bio.terra.workspace.service.workspace.WorkspaceService;
import bio.terra.workspace.service.workspace.exceptions.AzureContextRequiredException;
import com.azure.core.credential.TokenCredential;
import com.azure.core.management.AzureEnvironment;
import com.azure.core.management.profile.AzureProfile;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.resourcemanager.storage.StorageManager;
import com.azure.resourcemanager.storage.models.PublicAccess;
import com.azure.resourcemanager.storage.models.StorageAccountCreateParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static bio.terra.workspace.service.workspace.flight.WorkspaceFlightMapKeys.ControlledResourceKeys.CREATION_PARAMETERS;

public class CreateAzureStorageContainerStep implements Step {
    private static final Logger logger =
            LoggerFactory.getLogger(CreateAzureStorageContainerStep.class);
    private final ControlledAzureStorageContainerResource resource;
    private final WorkspaceService workspaceService;

    public CreateAzureStorageContainerStep(
            ControlledAzureStorageContainerResource resource, WorkspaceService workspaceService) {
        this.resource = resource;
        this.workspaceService = workspaceService;
    }


    @Override
    public StepResult doStep(FlightContext flightContext)
            throws InterruptedException, RetryException {
        FlightMap inputMap = flightContext.getInputParameters();
        ApiAzureStorageContainerCreationParameters creationParameters =
                inputMap.get(CREATION_PARAMETERS, ApiAzureStorageContainerCreationParameters.class);
        final var azureContext = workspaceService.getAzureCloudContext(resource.getWorkspaceId()).orElseThrow(() -> new AzureContextRequiredException("Azure not configured"));
        final var storageManager = AzureConfigurationUtilities.getAzureStorageManager(azureContext);

        //Create storage account if needed
        final var storageAccountClient = storageManager.serviceClient().getStorageAccounts();
        var storageAccount = storageAccountClient.getByResourceGroup(azureContext.getAzureResourceGroupId(), resource.getStorageAccountName());
        if (storageAccount != null) {
            logger.info("Storage account {} already exists and will not be re-created.", resource.getStorageAccountName());
        } else if (!storageManager.storageAccounts().checkNameAvailability(resource.getStorageAccountName()).isAvailable()) {
            logger.error("Storage account {} does not exist in the resource group {}, but the storage account name is unavailable.",
                    resource.getStorageAccountName(), azureContext.getAzureResourceGroupId());
            return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL);
        } else {
            StorageAccountCreateParameters params = new StorageAccountCreateParameters();
            //TODO: Add additional parameters (e.g. scope)
            try {
                storageAccountClient.create(
                        azureContext.getAzureResourceGroupId(),
                        resource.getStorageAccountName(),
                        params);
            } catch (Exception e) {
                return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, e);
            }
        }

        //Create container if needed
        final var containerClient = storageManager.serviceClient().getBlobContainers();

        var existingContainer = containerClient.get(
                azureContext.getAzureResourceGroupId(),
                resource.getStorageAccountName(),
                resource.getContainerName());
        if (existingContainer != null) {
            logger.info("Storage container {} already exists. Continuing.");
        } else {
            var containerLevelPublicAccess = resource.getAccessScope() == AccessScopeType.ACCESS_SCOPE_SHARED ? PublicAccess.CONTAINER : PublicAccess.NONE;
            storageManager.blobContainers().defineContainer(resource.getContainerName())
                    .withExistingStorageAccount(azureContext.getAzureResourceGroupId(), resource.getStorageAccountName())
                    .withPublicAccess(containerLevelPublicAccess)
                    .create();
        }

        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext flightContext) throws InterruptedException {
        final var azureContext = workspaceService.getAzureCloudContext(resource.getWorkspaceId()).orElseThrow(() -> new AzureContextRequiredException("Azure not configured"));
        final var storageManager = AzureConfigurationUtilities.getAzureStorageManager(azureContext);
        storageManager.blobContainers().delete(
                azureContext.getAzureResourceGroupId(),
                resource.getStorageAccountName(),
                resource.getContainerName()
        );
        return StepResult.getStepResultSuccess();
    }
}
