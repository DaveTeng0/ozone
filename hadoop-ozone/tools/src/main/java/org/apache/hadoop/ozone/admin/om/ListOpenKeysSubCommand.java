package org.apache.hadoop.ozone.admin.om;


import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.ozone.client.OzoneClientException;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.ServiceInfo;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import picocli.CommandLine;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;

/**
 * Handler of ozone admin om list-open-keys command.
 */
@CommandLine.Command(
    name = "list-open-key",
    description = "CLI command to list open keys from om.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class
)
public class ListOpenKeysSubCommand implements Callable<Void> {

  @CommandLine.ParentCommand
  private OMAdmin parent;

  @CommandLine.Option(
      names = {"-id", "--service-id"},
      description = "Ozone Manager Service ID",
      required = true
  )
  private String omServiceId;


  private OzoneManagerProtocol ozoneManagerClient;

  @Override
  public Void call() throws Exception {
    String volName = "vol1";
    String buckName = "buck1";
    String keyPrefix = "somepre";
    try {
      ozoneManagerClient =  parent.createOmClient(omServiceId);
      printOpenKeys(ozoneManagerClient.listOpenKeys(volName,buckName,keyPrefix));
    } catch (OzoneClientException ex) {
      System.out.printf("Error: %s", ex.getMessage());
    } finally {
      if (ozoneManagerClient != null) {
        ozoneManagerClient.close();
      }
    }
    return null;

  }

  private void printOpenKeys(List<OmKeyInfo> openKeysList) {
    for(OmKeyInfo keyInfo : openKeysList) {
      System.out.println(
          keyInfo.getKeyName() + " : " +
              keyInfo.getObjectInfo());
    }

  }
}
