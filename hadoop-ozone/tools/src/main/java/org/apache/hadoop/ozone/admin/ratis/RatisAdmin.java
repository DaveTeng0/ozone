/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.admin.ratis;

import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.cli.OzoneAdmin;
import org.apache.hadoop.hdds.cli.SubcommandWithParent;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.client.OzoneClientException;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.kohsuke.MetaInfServices;

import org.apache.ratis.shell.cli.sh.command.GroupCommand;

import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

import java.util.Collection;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY;

/**
 * Subcommand for admin operations related to OM.
 */
@CommandLine.Command(
    name = "ratis",
    description = "Ozone ratis specific admin operations",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class,
    subcommands = {
//        LocalSubCommand.class,
        OzoneRatisGroupCommand.class
//        GroupCommand.class
    })
@MetaInfServices(SubcommandWithParent.class)
public class RatisAdmin extends GenericCli implements SubcommandWithParent {

  @CommandLine.ParentCommand
  private OzoneAdmin parent;

  @Spec
  private CommandSpec spec;

  public OzoneAdmin getParent() {
    return parent;
  }

  @Override
  public Void call() throws Exception {
    GenericCli.missingSubcommand(spec);

//    String[] args = {"ratis", "sh",""}; // ratis sh local raftMetaConf -peers <P0_HOST:P0_PORT,>
//    final RatisShell shell = new RatisShell(System.out);
//    System.exit(shell.run(args));

    return null;
  }

  public ClientProtocol createClient(String omServiceId) throws Exception {
    OzoneConfiguration conf = parent.getOzoneConf();
    if (OmUtils.isOmHAServiceId(conf, omServiceId)) {
      return OzoneClientFactory.getRpcClient(omServiceId, conf).getObjectStore()
        .getClientProxy();
    } else {
      throw new OzoneClientException("This command works only on OzoneManager" +
            " HA cluster. Service ID specified does not match" +
            " with " + OZONE_OM_SERVICE_IDS_KEY + " defined in the " +
            "configuration. Configured " + OZONE_OM_SERVICE_IDS_KEY + " are " +
            conf.getTrimmedStringCollection(OZONE_OM_SERVICE_IDS_KEY) + "\n");
    }
  }

  private String getTheOnlyConfiguredOmServiceIdOrThrow() {
    if (getConfiguredServiceIds().size() != 1) {
      throw new IllegalArgumentException("There is no Ozone Manager service ID "
          + "specified, but there are either zero, or more than one service "
          + "configured. Please specify the service ID to be finalized.");
    }
    return getConfiguredServiceIds().iterator().next();
  }

  private Collection<String> getConfiguredServiceIds() {
    OzoneConfiguration conf = parent.getOzoneConf();
    Collection<String> omServiceIds =
        conf.getTrimmedStringCollection(OZONE_OM_SERVICE_IDS_KEY);
    return omServiceIds;
  }

  @Override
  public Class<?> getParentType() {
    return OzoneAdmin.class;
  }
}
