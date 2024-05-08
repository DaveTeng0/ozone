package org.apache.hadoop.ozone.admin.ratis;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.client.OzoneClientException;
import org.apache.hadoop.ozone.om.protocolPB.Hadoop3OmTransportFactory;
import org.apache.hadoop.ozone.om.protocolPB.OmTransport;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ratis.protocol.ClientId;

import java.util.Collection;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY;

public class RatisUtil {



  public static OzoneManagerProtocolClientSideTranslatorPB createOmClient(
      String omServiceID, OzoneConfiguration ozoneConfiguration, UserGroupInformation ugi
  ) throws Exception {
    return createOmClient(omServiceID, null, true, ozoneConfiguration, ugi);
  }

  public static OzoneManagerProtocolClientSideTranslatorPB createOmClient(
      String omServiceID,
      String omHost,
      boolean forceHA,
      OzoneConfiguration ozoneConfiguration,
      UserGroupInformation ugi
  ) throws Exception {
    OzoneConfiguration conf = ozoneConfiguration;
    if (omHost != null && !omHost.isEmpty()) {
      omServiceID = null;
      conf.set(OZONE_OM_ADDRESS_KEY, omHost);
    } else if (omServiceID == null || omServiceID.isEmpty()) {
      omServiceID = getTheOnlyConfiguredOmServiceIdOrThrow(conf);
    }
    RPC.setProtocolEngine(conf, OzoneManagerProtocolPB.class,
        ProtobufRpcEngine.class);
    String clientId = ClientId.randomId().toString();
    if (!forceHA || (forceHA && OmUtils.isOmHAServiceId(conf, omServiceID))) {
      OmTransport omTransport = new Hadoop3OmTransportFactory()
          .createOmTransport(conf, ugi, omServiceID);
      return new OzoneManagerProtocolClientSideTranslatorPB(omTransport,
          clientId);
    } else {
      throw new OzoneClientException("This command works only on OzoneManager" +
          " HA cluster. Service ID specified does not match" +
          " with " + OZONE_OM_SERVICE_IDS_KEY + " defined in the " +
          "configuration. Configured " + OZONE_OM_SERVICE_IDS_KEY + " are " +
          conf.getTrimmedStringCollection(OZONE_OM_SERVICE_IDS_KEY) + "\n");
    }
  }

  private static String getTheOnlyConfiguredOmServiceIdOrThrow(OzoneConfiguration configuration) {
    if (getConfiguredServiceIds(configuration).size() != 1) {
      throw new IllegalArgumentException("There is no Ozone Manager service ID "
          + "specified, but there are either zero, or more than one service ID"
          + "configured.");
    }
    return getConfiguredServiceIds(configuration).iterator().next();
  }

  private static Collection<String> getConfiguredServiceIds(OzoneConfiguration configuration) {
    OzoneConfiguration conf = configuration;
    Collection<String> omServiceIds =
        conf.getTrimmedStringCollection(OZONE_OM_SERVICE_IDS_KEY);
    return omServiceIds;
  }

}
