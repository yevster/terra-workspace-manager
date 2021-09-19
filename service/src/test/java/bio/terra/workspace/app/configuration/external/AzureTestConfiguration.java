package bio.terra.workspace.app.configuration.external;

import java.util.ArrayList;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties
@ConfigurationProperties(prefix = "workspace.azure-test")
public class AzureTestConfiguration {
  private List<AzureUser> azureUsers = new ArrayList<>();

  public List<AzureUser> getAzureUsers() {
    return azureUsers;
  }

  public void setAzureUsers(List<AzureUser> azureUsers) {
    this.azureUsers = azureUsers;
  }

  public static class AzureUser {
    private String email;
    private String objectId;

    public String getEmail() {
      return email;
    }

    public void setEmail(String email) {
      this.email = email;
    }

    public String getObjectId() {
      return objectId;
    }

    public void setObjectId(String objectId) {
      this.objectId = objectId;
    }
  }
}
