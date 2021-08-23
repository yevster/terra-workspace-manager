package bio.terra.workspace.service.workspace.flight;

/**
 * Contract for a key-value pair in a Step.
 */
public interface StepContract {
  FlightMapKey getKey();
  FlightMapType getFlightMapType();
  boolean getIsRequired();
  FlightMapEntryIOType getIOType();
}
