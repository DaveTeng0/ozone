package org.apache.hadoop.ozone.admin.ratis;


import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.ratis.RatisHelper;
import org.apache.hadoop.hdds.scm.client.ClientTrustManager;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.client.CACertificateProvider;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.ozone.admin.om.OMAdmin;
import org.apache.hadoop.ozone.client.OzoneClientException;
import org.apache.hadoop.ozone.om.helpers.ServiceInfoEx;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.protocolPB.*;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.GroupManagementApi;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.grpc.GrpcTlsConfig;
import org.apache.ratis.netty.NettyConfigKeys;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.*;
import org.apache.ratis.protocol.exceptions.RaftException;
import org.apache.ratis.security.TlsConf;
import org.apache.ratis.shell.cli.sh.command.AbstractRatisCommand;
import org.apache.ratis.shell.cli.RaftUtils;

import org.apache.ratis.util.function.CheckedFunction;
import picocli.CommandLine;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.URL;
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
  public OzoneRatisGroupCommand parent;

  @CommandLine.Option(names = {"-peers"},
      description = "list of peers",
      required = true)
  private String peers;

  @CommandLine.Option(names = { "-groupid" },
      defaultValue = "test_group",
      description = "groupid")
  private String groupid;

  @CommandLine.Option(names = {"-id", "--service-id"},
      description = "OM Service ID",
      required = false)
  private String omServiceId;

  private OzoneManagerProtocol ozoneManagerClient;

  private RaftGroup raftGroup;
  private GroupInfoReply groupInfoReply;

//  private final PrintStream printStream;

  OzoneConfiguration conf;
  ClientId clientId = ClientId.randomId();
  UserGroupInformation ugi;

  static ClassLoader CLASS_LOADER = OzoneRatisGroupInfoCommand.class.getClassLoader();


  static File getResource(String name) {
    final File file = Optional.ofNullable(CLASS_LOADER.getResource(name))
        .map(URL::getFile)
        .map(File::new)
        .orElse(null);
//    LOG.info("Getting resource {}: {}", name, file);
    return file;
  }

  public void tls() throws Exception{
    final Parameters parameters = new Parameters();
//    final TlsConf serverTlsConfig = SecurityTestUtils.newServerTlsConfig(true);
//    TlsConf serverTlsConfig = new TlsConf.Builder()
//        .setName("server")
//        .setPrivateKey(new TlsConf.PrivateKeyConf(getResource("ssl/server.pem")))
//        .setKeyCertificates(new TlsConf.CertificatesConf(getResource("ssl/server.crt")))
//        .setTrustCertificates(new TlsConf.CertificatesConf(getResource("ssl/client.crt")))
//        .setMutualTls(true)
//        .build();
//
//    NettyConfigKeys.DataStream.Server.setTlsConf(parameters, serverTlsConfig);
//    TlsConf clientTlsConfig = SecurityTestUtils.newClientTlsConfig(true);
//    TlsConf clientTlsConfig = new TlsConf.Builder()
//        .setName("client")
//        .setPrivateKey(new TlsConf.PrivateKeyConf(getResource("cm-auto-in_cluster_ca_cert.pem")))
//        .setKeyCertificates(new TlsConf.CertificatesConf(getResource("ssl/client.crt")))
//        .setTrustCertificates(new TlsConf.CertificatesConf(getResource("ssl/ca.crt")))
//        .setMutualTls(true)
//        .build();
//    ServiceInfoEx serviceInfoEx = ozoneManagerClient.getServiceInfo();

    ServiceInfoEx serviceInfoEx = ozoneManagerClient.getServiceInfo();
    CACertificateProvider remoteCAProvider =
        () -> ozoneManagerClient.getServiceInfo().provideCACerts();
    ClientTrustManager trustManager = new ClientTrustManager(remoteCAProvider, serviceInfoEx);
//    ClientTrustManager trustManager = new ClientTrustManager(remoteCAProvider, null);


    final GrpcTlsConfig tlsConfig = RatisHelper.createTlsClientConfig(new
        SecurityConfig(parent.getParent().getParent().getOzoneConf()), trustManager);

    NettyConfigKeys.DataStream.Client.setTlsConf(parameters, tlsConfig);

  }

  protected OmTransport createOmTransport(String omServiceId)
      throws IOException {
    return OmTransportFactory.create(conf, ugi, omServiceId);
  }


  @Override
  public Void call() throws Exception {
    try {
//      ozoneManagerClient =  parent.getParent().createOmClient(omServiceId);


//      this.conf = parent.getParent().getParent().getOzoneConf();
//      OmTransport omTransport = createOmTransport(omServiceId);
//      OzoneManagerProtocolClientSideTranslatorPB
//          ozoneManagerProtocolClientSideTranslatorPB =
//          new OzoneManagerProtocolClientSideTranslatorPB(omTransport,
//              clientId.toString());
//      this.ozoneManagerClient = TracingUtil.createProxy(
//          ozoneManagerProtocolClientSideTranslatorPB,
//          OzoneManagerClientProtocol.class, conf);

      this.ugi = UserGroupInformation.getCurrentUser();
      System.out.println("*****______________orgc.call, ugi:  " + ugi);
//      System.out.println("*****______________orgc.call, conf:  " + conf.toString());
//      ozoneManagerClient =  RatisUtil.createOmClient(omServiceId, conf, ugi);

//      if (json) {
//        printOmServerRolesAsJson(ozoneManagerClient.getServiceList());
//      } else {
//        printOmServerRoles(ozoneManagerClient.getServiceList());
//      }

//      tls();

      t();

      p();
    } catch (Exception ex) {
      StringWriter writer = new StringWriter();
      PrintWriter printWriter = new PrintWriter( writer );
      ex.printStackTrace( printWriter );
      printWriter.flush();

      String stackTrace = writer.toString();
      System.out.printf("Error: %s", stackTrace);
    } finally {
      if (ozoneManagerClient != null) {
        ozoneManagerClient.close();
      }
    }
    return null;
  }

  public void p() {
    println("*****_______ ORGC.p-01, group id: " + raftGroup.getGroupId().getUuid());
    final GroupInfoReply reply = groupInfoReply;
    RaftProtos.RaftPeerProto leader = getLeader(reply == null ? null : reply.getRoleInfoProto());
    if (leader == null) {
      println("leader not found");
    } else {
      println(String.format("leader info: %s(%s)%n%n", leader.getId().toStringUtf8(), leader.getAddress()));
    }
    println(reply != null ? reply.getCommitInfos().toString() : null);

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


  public int t_v1() throws Exception{
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
      println(String.format("******__________ ORGIC.t_v1-01,,,, %s. %n", groupInfoReply));

      processReply(groupInfoReply,
          () -> "Failed to get group info for group id " + remoteGroupId.getUuid() + " from " + peers);
      raftGroup = groupInfoReply.getGroup();
    }
    return 0;

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

    System.out.println("*****_______ GrpcOmTransport.setCaCerts => " + GrpcOmTransport.getCaCerts().size());

    try (final RaftClient client = RaftUtils.createClient(raftGroup)) {
      final RaftGroupId remoteGroupId;
      if (raftGroupIdFromConfig != AbstractRatisCommand.DEFAULT_RAFT_GROUP_ID) {
        remoteGroupId = raftGroupIdFromConfig;
      } else {
//        final List<RaftGroupId> groupIds = run(peers,
//            p -> client.getGroupManagementApi((p.getId())).list().getGroupIds()
////            p -> ozoneManagerClient.getGroupManagementApi((p.getId())).list().getGroupIds()
//        );

        final List<RaftGroupId> groupIds = run(peers,
            p -> {
              GroupManagementApi api = client.getGroupManagementApi((p.getId()));
              println("******_________ arc.run-01,");
              GroupListReply r;
              try {
                r = api.list();
              } catch (Exception e) {
                println("******_________ arc.run-03-catch-err, e = " + e.toString());
                throw e;
              }
              println("******_________ arc.run-02, GroupListReply = " + r);

              List<RaftGroupId> ls = r.getGroupIds();
              return ls;
            });

        println(String.format("******__________ ORGIC.t()-01,,,, remoteGroupId = %s. %n", groupIds != null ? Arrays.toString(groupIds.toArray()) : null));

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
      println(String.format("******__________ ORGIC.t()-02,,,, remoteGroupId = %s. %n", remoteGroupId));

      groupInfoReply = run(peers, p -> client.getGroupManagementApi((p.getId())).info(remoteGroupId));
      println(String.format("******__________ ORGIC.t()-03,,,, %s. %n", groupInfoReply));

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
      println(String.format("******__________ ORGIC.pr-01,,,, %s. Error: %s%n", message, e));
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

  public void setPeers(String peers) {
    this.peers = peers;
  }

  public String getPeers() {
    return peers;
  }

//  @Override // even override here, mvn still complaint
//org.apache.hadoop.ozone.admin.ratis.OzoneRatisGroupInfoCommand is not abstract
// and does not override abstract method getDescription()
// in org.apache.ratis.shell.cli.Command

//  public String description() {
//    return "Print ratis group info";
//  }

}
