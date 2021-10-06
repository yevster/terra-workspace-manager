package bio.terra.workspace.service.resource.controlled.flight.create.azure;

import bio.terra.workspace.service.workspace.model.AzureCloudContext;
import com.azure.core.credential.TokenCredential;
import com.azure.core.management.AzureEnvironment;
import com.azure.core.management.profile.AzureProfile;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.resourcemanager.storage.StorageManager;
import java.lang.reflect.Field;
import org.apache.commons.lang3.StringUtils;

public class AzureConfigurationUtilities {
  public static AzureEnvironment getAzureEnvironmentByName(String azureEnvironment) {
    if (StringUtils.isBlank(azureEnvironment)) return AzureEnvironment.AZURE;
    try {
      Field environmentConstant = AzureEnvironment.class.getDeclaredField(azureEnvironment);
      return (AzureEnvironment) environmentConstant.get(null);
    } catch (NoSuchFieldException nsfe) {
      throw new IllegalArgumentException("Unable to find Azure environment: " + azureEnvironment);
    } catch (IllegalAccessException iae) { // Should never happen unless SDK changes
      throw new IllegalStateException("Unable to access available azure environment names");
    }
  }

  public static StorageManager getAzureStorageManager(AzureCloudContext azureCloudContext) {
    TokenCredential azureCredential =
        new DefaultAzureCredentialBuilder().tenantId(azureCloudContext.getAzureTenantId()).build();

    // Does the storage account exist? If not create it.
    AzureEnvironment azureEnvironment =
        getAzureEnvironmentByName(azureCloudContext.getAzureEnvironment());

    return StorageManager.authenticate(
        azureCredential,
        new AzureProfile(
            azureCloudContext.getAzureTenantId(),
            azureCloudContext.getAzureSubscriptionId(),
            azureEnvironment));
  }
}
