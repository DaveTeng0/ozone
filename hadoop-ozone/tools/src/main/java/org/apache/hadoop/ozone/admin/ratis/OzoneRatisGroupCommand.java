package org.apache.hadoop.ozone.admin.ratis;

import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.cli.OzoneAdmin;
import org.apache.hadoop.hdds.cli.SubcommandWithParent;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.server.JsonUtils;
import org.apache.hadoop.ozone.admin.om.*;
import org.apache.hadoop.ozone.client.OzoneClientException;
import org.apache.hadoop.ozone.om.helpers.ServiceInfo;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.ratis.shell.cli.Command;
import org.apache.ratis.shell.cli.sh.command.Context;
import org.apache.ratis.shell.cli.sh.command.GroupCommand;
import org.apache.ratis.shell.cli.sh.group.GroupInfoCommand;
import org.apache.ratis.shell.cli.sh.group.GroupListCommand;
import org.kohsuke.MetaInfServices;
import picocli.CommandLine;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.function.Function;

/**
 * Subcommand for admin operations related to OM.
 */
@CommandLine.Command(
    name = "group-test",
    description = "ratis group operations",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class,
    subcommands = {
        OzoneRatisGroupInfoCommand.class
    })
//@MetaInfServices(SubcommandWithParent.class)

public class OzoneRatisGroupCommand extends GenericCli implements SubcommandWithParent
//    extends org.apache.ratis.shell.cli.sh.command.GroupCommand
{

  @CommandLine.ParentCommand
  private RatisAdmin parent;

  @CommandLine.Spec
  private CommandLine.Model.CommandSpec spec;

  public RatisAdmin getParent() {
    return parent;
  }

  @Override
  public Void call() throws Exception {
    GenericCli.missingSubcommand(spec);
    return null;
  }

  @Override
  public Class<?> getParentType() {
    return RatisAdmin.class;
  }


}
