package bio.terra.workspace.common;

import bio.terra.workspace.app.configuration.external.AzureTestConfiguration;
import bio.terra.workspace.app.configuration.external.AzureTestConfiguration.AzureUser;
import bio.terra.workspace.db.IamDao;
import bio.terra.workspace.db.IamDao.PocUser;
import bio.terra.workspace.service.iam.AuthenticatedUserRequest;
import bio.terra.workspace.service.iam.AuthenticatedUserRequest.AuthType;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class AzureTestUtils {

  private final AzureTestConfiguration azureTestConfiguration;
  private final IamDao iamDao;

  @Autowired
  public AzureTestUtils(AzureTestConfiguration azureTestConfiguration, IamDao iamDao) {
    this.azureTestConfiguration = azureTestConfiguration;
    this.iamDao = iamDao;
  }

  public void loadUsersFromConfiguration() {
    for (AzureUser user : azureTestConfiguration.getAzureUsers()) {
      PocUser pocUser = new PocUser().userId(user.getObjectId()).email(user.getEmail());
      iamDao.addUser(pocUser);
    }
  }

  public String getTestUserEmail(int index) {
    AzureUser user = azureTestConfiguration.getAzureUsers().get(index);
    return user.getEmail();
  }

  public AuthenticatedUserRequest getUserRequest(int index) {
    AzureUser user = azureTestConfiguration.getAzureUsers().get(index);
    return new AuthenticatedUserRequest()
        .authType(AuthType.BASIC)
        .email(user.getEmail())
        .reqId(UUID.randomUUID())
        .subjectId(user.getObjectId());
  }
}
