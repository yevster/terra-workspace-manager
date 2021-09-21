package bio.terra.workspace.service.workspace.exceptions;

import bio.terra.common.exception.BadRequestException;

/** You must provide the azure context when creating azure cloud context */
public class AzureContextRequiredException extends BadRequestException {
  public AzureContextRequiredException(String message) {
    super(message);
  }
}
