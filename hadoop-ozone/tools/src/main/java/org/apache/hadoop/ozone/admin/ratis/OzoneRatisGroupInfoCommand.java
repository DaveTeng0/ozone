package org.apache.hadoop.ozone.admin.ratis;


import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.ozone.admin.om.OMAdmin;
import org.apache.hadoop.ozone.client.OzoneClientException;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.exceptions.RaftException;
import org.apache.ratis.shell.cli.sh.command.AbstractRatisCommand;
import org.apache.ratis.shell.cli.RaftUtils;

import org.apache.ratis.protocol.GroupInfoReply;

import org.apache.ratis.util.function.CheckedFunction;
import picocli.CommandLine;

import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@CommandLine.Command(
    name = "info",
    description = "print Ratis server info",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)

public class OzoneRatisGroupInfoCommand
//    extends AbstractRatisCommand
    implements Callable<Void> {

  @CommandLine.ParentCommand
  private OzoneRatisGroupCommand parent;

  @CommandLine.Option(names = {"-peers"},
      description = "list of peers",
      required = true)
  private String peers;

  @CommandLine.Option(names = { "-groupid" },
      defaultValue = "false",
      description = "groupid")
  private boolean groupid;

  private OzoneManagerProtocol ozoneManagerClient;

  private RaftGroup raftGroup;
  private GroupInfoReply groupInfoReply;

//  private final PrintStream printStream;

  @Override
  public Void call() throws Exception {
    try {
//      ozoneManagerClient =  parent.createOmClient(omServiceId);
//      if (json) {
//        printOmServerRolesAsJson(ozoneManagerClient.getServiceList());
//      } else {
//        printOmServerRoles(ozoneManagerClient.getServiceList());
//      }

      t();

      p();
    } catch (Exception ex) {
      System.out.printf("Error: %s", ex.getMessage());
    } finally {
      if (ozoneManagerClient != null) {
        ozoneManagerClient.close();
      }
    }
    return null;
  }

  public void p() {
    println("group id: " + raftGroup.getGroupId().getUuid());
    final GroupInfoReply reply = groupInfoReply;
    RaftProtos.RaftPeerProto leader = getLeader(reply.getRoleInfoProto());
    if (leader == null) {
      println("leader not found");
    } else {
      println(String.format("leader info: %s(%s)%n%n", leader.getId().toStringUtf8(), leader.getAddress()));
    }
    println(reply.getCommitInfos().toString());

  }

  protected RaftProtos.RaftPeerProto getLeader(
      RaftProtos.RoleInfoProto roleInfo) {
    if (roleInfo == null) {
      return null;
    }
    if (roleInfo.getRole() == RaftProtos.RaftPeerRole.LEADER) {
      return roleInfo.getSelf();
    }
    RaftProtos.FollowerInfoProto followerInfo = roleInfo.getFollowerInfo();
    if (followerInfo == null) {
      return null;
    }
    return followerInfo.getLeaderInfo().getId();
  }


  public int t() throws Exception{
    List<InetSocketAddress> addresses = new ArrayList<>();
    String peersStr = peers;
    String[] peersArray = peersStr.split(",");
    for (String peer : peersArray) {
      addresses.add(AbstractRatisCommand.parseInetSocketAddress(peer));
    }

    final RaftGroupId raftGroupIdFromConfig =
//        cl.hasOption(GROUPID_OPTION_NAME)?
//        RaftGroupId.valueOf(UUID.fromString(cl.getOptionValue(GROUPID_OPTION_NAME))) :
    AbstractRatisCommand.DEFAULT_RAFT_GROUP_ID;

    List<RaftPeer> peers = addresses.stream()
        .map(addr -> RaftPeer.newBuilder()
            .setId(RaftUtils.getPeerId(addr))
            .setAddress(addr)
            .build()
        ).collect(Collectors.toList());
    raftGroup = RaftGroup.valueOf(raftGroupIdFromConfig, peers);
    try (final RaftClient client = RaftUtils.createClient(raftGroup)) {
      final RaftGroupId remoteGroupId;
      if (raftGroupIdFromConfig != AbstractRatisCommand.DEFAULT_RAFT_GROUP_ID) {
        remoteGroupId = raftGroupIdFromConfig;
      } else {
        final List<RaftGroupId> groupIds = run(peers,
            p -> client.getGroupManagementApi((p.getId())).list().getGroupIds());

        if (groupIds == null) {
          println("Failed to get group ID from " + peers);
          return -1;
        } else if (groupIds.size() == 1) {
          remoteGroupId = groupIds.get(0);
        } else {
          println("There are more than one groups, you should specific one. " + groupIds);
          return -2;
        }
      }

      groupInfoReply = run(peers, p -> client.getGroupManagementApi((p.getId())).info(remoteGroupId));
      processReply(groupInfoReply,
          () -> "Failed to get group info for group id " + remoteGroupId.getUuid() + " from " + peers);
      raftGroup = groupInfoReply.getGroup();
    }
    return 0;

  }


  protected void processReply(RaftClientReply reply, Supplier<String> messageSupplier) throws
      IOException {
    if (reply == null || !reply.isSuccess()) {
      final RaftException e = Optional.ofNullable(reply)
          .map(RaftClientReply::getException)
          .orElseGet(() -> new RaftException("Reply: " + reply));
      final String message = messageSupplier.get();
      println(String.format("%s. Error: %s%n", message, e));
      throw new IOException(message, e);
    }
  }

  protected void println(String message) {
//    printStream.println(message);
    System.out.println("****______ test => " + message);
  }

  public <T, K, E extends Throwable> K run(Collection<T> list, CheckedFunction<T, K, E> function) {
    for (T t : list) {
      try {
        K ret = function.apply(t);
        if (ret != null) {
          return ret;
        }
      } catch (Throwable e) {
        e.printStackTrace();
      }
    }
    return null;
  }


//  @Override // even override here, mvn still complaint
//org.apache.hadoop.ozone.admin.ratis.OzoneRatisGroupInfoCommand is not abstract
// and does not override abstract method getDescription()
// in org.apache.ratis.shell.cli.Command

//  public String description() {
//    return "Print ratis group info";
//  }

}
