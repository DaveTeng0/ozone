package org.apache.hadoop.ozone.client.io;

import org.apache.hadoop.fs.SafeModeAction;
import org.apache.hadoop.hdds.client.*;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ContainerClientMetrics;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.storage.BlockOutputStream;
import org.apache.hadoop.io.ByteBufferPool;
import org.apache.hadoop.io.ElasticByteBufferPool;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.*;
import org.apache.hadoop.ozone.om.protocol.S3Auth;
import org.apache.hadoop.ozone.om.protocolPB.OmTransportFactory;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerClientProtocol;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.EchoRPCResponse;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalizer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.ozone.test.TestClock;
import org.apache.ratis.protocol.ClientId;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.mockito.Mockito.*;

public class TestKeyOutputStream {

  private int chunkSize;
  private int blockSize;

  private String keyString;

  public void setup() {
    chunkSize = (int) OzoneConsts.MB;
    blockSize = 4 * chunkSize;
    keyString = UUID.randomUUID().toString();

  }
  @Test
  public void t1__RATIS() throws Exception{ // nnnnnnnnnnnnnnnnnnn
    KeyOutputStream keyOutputStream =
        createKeyOutputStream(ReplicationType.RATIS);
//    OzoneOutputStream keyOutputStream =
//        createKeyOutputStream(ReplicationType.RATIS);



    byte[] data = "test".getBytes(UTF_8);
//    keyOutputStream.getBlockOutputStreamEntryPool()
//        .getCurrentStreamEntry().getOutputStream().write(4);

    keyOutputStream.write(data);

//    List<OmKeyLocationInfo> locationInfoList =
//        keyOutputStream.getLocationInfoList();
//
//    long containerId = locationInfoList.get(0).getContainerID();
//    ContainerInfo container = cluster.getStorageContainerManager()
//        .getContainerManager()
//        .getContainer(ContainerID.valueOf(containerId));
//    Pipeline pipeline =
//        cluster.getStorageContainerManager().getPipelineManager()
//            .getPipeline(container.getPipelineID());
//    List<DatanodeDetails> datanodes = pipeline.getNodes();

//    cluster.shutdownHddsDatanode(datanodes.get(0));
//    cluster.shutdownHddsDatanode(datanodes.get(1));

//    keyOutputStream.flush();

    Assert.assertEquals(3,
        keyOutputStream.getExcludeList().getDatanodes().size());

    ExcludeList excludeList = spy(keyOutputStream.getExcludeList());
    doReturn(true).when(excludeList).isExpired(anyLong());
    keyOutputStream.setExcludeList(excludeList);
    Assert.assertEquals(0,
        keyOutputStream.getExcludeList().getDatanodes().size());

  }

  @Test
  public void testECKeyOutputStreamExpiryTime() throws Exception {
    KeyOutputStream keyOutputStream =
        createECKeyOutputStream();
    byte[] data = "test".getBytes(UTF_8);
    keyOutputStream.write(data);
    Assert.assertEquals(3,
        keyOutputStream.getExcludeList().getDatanodes().size());

    ExcludeList excludeList = spy(keyOutputStream.getExcludeList());
    doReturn(true).when(excludeList).isExpired(anyLong());
    keyOutputStream.setExcludeList(excludeList);
    Assert.assertEquals(0,
        keyOutputStream.getExcludeList().getDatanodes().size());

  }

  private KeyOutputStream createKeyOutputStream(ReplicationType type) throws Exception {
//    private OzoneOutputStream createKeyOutputStream(ReplicationType type) throws Exception {

    OpenKeySession mockOpenKeySession = mock(OpenKeySession.class);
//    when(mockOpenKeySession.getId()).thenReturn(1L);
    doReturn(1L).when(mockOpenKeySession).getId();
    OmKeyInfo omKeyInfo =  new OmKeyInfo.Builder()
        .setBucketName("bucket")
        .setVolumeName("volume")
        .setKeyName("someKey")
        .build();
    doReturn(omKeyInfo).when(mockOpenKeySession).getKeyInfo();

    XceiverClientFactory xceiverClientManager = mock(XceiverClientFactory.class);

    OzoneManagerClientProtocol ozoneManagerClient = mock(OzoneManagerClientProtocol.class);
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    OzoneConfiguration conf = new OzoneConfiguration();
//    MockOzoneManagerProtocolClientSideTranslatorPB ozoneManagerClient =
//        new MockOzoneManagerProtocolClientSideTranslatorPB();

//    OzoneManagerClientProtocol ozoneManagerClient = mock(OzoneManagerProtocolClientSideTranslatorPB.class);

    Pipeline pipeline = new Pipeline.Builder()
//        .setId(new PipelineID(UUID.randomUUID()))
        .setState(Pipeline.PipelineState.OPEN)
        .setReplicationConfig(RatisReplicationConfig.getInstance(THREE))
        .setId(PipelineID.randomId())
//        .setNodes(Arrays.asList(new DatanodeDetails()))
        .setNodes(new ArrayList<>())

        .build();

//    pipeline.getReplicaIndex(pipeline.getClosestNode())
    OmKeyLocationInfo keyLocationInfo = new OmKeyLocationInfo.Builder()
        .setPipeline(pipeline)
        .build();
//    when(ozoneManagerClient.allocateBlock(any(),any(),any())).thenReturn(keyLocationInfo);
//    doReturn(keyLocationInfo).when(ozoneManagerClient).allocateBlock(any(),any(),any());

    OzoneClientConfig clientConfig = mock(OzoneClientConfig.class);
    doReturn(1).when(clientConfig).getStreamBufferSize();
    doReturn(300 * 1000L).when(clientConfig).getExcludeNodesExpiryTime();

    KeyOutputStream.Builder builder;

//    if (type.equals(HddsProtos.ReplicationType.EC)) {
    if (type.equals(ReplicationType.EC)) {
      ECReplicationConfig ecReplicationConfig =
          new ECReplicationConfig(3, 2, ECReplicationConfig.EcCodec.RS, (int) OzoneConsts.MB);

      builder = new ECKeyOutputStream.Builder()
          .setReplicationConfig(ecReplicationConfig);
    } else {
      ReplicationConfig repConfig =
          RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE);

      builder = new KeyOutputStream.Builder()
          .setReplicationConfig(repConfig);
    }

    KeyOutputStream keyOutputStream =
        spy(builder.setHandler(mockOpenKeySession)
        .setXceiverClientManager(xceiverClientManager)
        .setOmClient(ozoneManagerClient)
        .setConfig(clientConfig)
        .build());

    BlockOutputStreamEntryPool blp = spy(keyOutputStream.getBlockOutputStreamEntryPool());
    BlockOutputStreamEntry ble = mock(BlockOutputStreamEntry.class);
//    BlockOutputStreamEntry ble2 = spy(new BlockOutputStreamEntry.Builder()
//        .setBlockID(new BlockID(1,1))
//        .setKey("test")
////        .setXceiverClientManager(xceiverClientFactory)
////        .setPipeline(subKeyInfo.getPipeline())
////        .setConfig(config)
////        .setLength(subKeyInfo.getLength())
////        .setBufferPool(bufferPool)
////        .setToken(subKeyInfo.getToken())
////        .setClientMetrics(clientMetrics)
//        .build());

//    current.write(b, off, writeLen);
//    streamEntry.getBlockID().getContainerID();
    doThrow(IOException.class).when(ble).write(any(byte[].class),anyInt(),anyInt());
    doReturn(pipeline).when(ble).getPipeline();
    doReturn(new BlockID(1,1)).when(ble).getBlockID();
//    streamEntry.getFailedServers
    List<DatanodeDetails> dns = new ArrayList<>(3);
    for (int i = 0; i < 3; i++) {
      dns.add(
          DatanodeDetails.getFromProtoBuf(HddsProtos.DatanodeDetailsProto.newBuilder().setUuid128(
              HddsProtos.UUID.newBuilder().setLeastSigBits(i).setMostSigBits(i)
                  .build()).setHostName("localhost").setIpAddress("1.2.3.4")
          .addPorts(HddsProtos.Port.newBuilder().setName("RATIS").setValue(i)
              .build()).build())
      );
    }
//    DatanodeDetails.getFromProtoBuf(dns.get(i)))
    doReturn(dns).when(ble).getFailedServers();


    doReturn(ble).when(blp).allocateBlockIfNeeded();
    doReturn(4L).when(blp).getKeyLength();
    keyOutputStream.setBlockOutputStreamEntryPool(blp);

    doReturn(4)
        .when(keyOutputStream)
        .getDataWritten(any(BlockOutputStreamEntry.class),anyLong());
//    doNothing().when(keyOutputStream).write(any());;
    return keyOutputStream;
//    return new OzoneOutputStream(keyOutputStream,false);
  }

  protected KeyOutputStream createECKeyOutputStream() throws Exception {
    OpenKeySession mockOpenKeySession = mock(OpenKeySession.class);
    doReturn(1L).when(mockOpenKeySession).getId();
    OmKeyInfo omKeyInfo =  new OmKeyInfo.Builder()
        .setBucketName("bucket")
        .setVolumeName("volume")
        .setKeyName("someKey")
        .build();
    doReturn(omKeyInfo).when(mockOpenKeySession).getKeyInfo();

    XceiverClientFactory xceiverClientManager = mock(XceiverClientFactory.class);

    OzoneManagerClientProtocol ozoneManagerClient = mock(OzoneManagerClientProtocol.class);

    Pipeline pipeline = new Pipeline.Builder()
//        .setId(new PipelineID(UUID.randomUUID()))
        .setState(Pipeline.PipelineState.OPEN)
        .setReplicationConfig(RatisReplicationConfig.getInstance(THREE))
        .setId(PipelineID.randomId())
//        .setNodes(Arrays.asList(new DatanodeDetails()))
        .setNodes(new ArrayList<>())
        .build();

    OzoneClientConfig clientConfig = mock(OzoneClientConfig.class);
    doReturn(1).when(clientConfig).getStreamBufferSize();
    doReturn(300 * 1000L).when(clientConfig).getExcludeNodesExpiryTime();

    KeyOutputStream.Builder builder;
    ECReplicationConfig ecReplicationConfig =
        new ECReplicationConfig(3, 2, ECReplicationConfig.EcCodec.RS, (int) OzoneConsts.MB);

    ByteBufferPool byteBufferPool = mock(ElasticByteBufferPool.class);
    ByteBuffer bf = spy(ByteBuffer.allocate(1));
    doReturn(bf).when(byteBufferPool).getBuffer(anyBoolean(),anyInt());
//    doReturn(bf).when(bf).limit(anyInt());
    builder = new ECKeyOutputStream.Builder()
        .setReplicationConfig(ecReplicationConfig)
        .setByteBufferPool(byteBufferPool);
    ;

    KeyOutputStream keyOutputStream =
        spy(builder.setHandler(mockOpenKeySession)
            .setXceiverClientManager(xceiverClientManager)
            .setOmClient(ozoneManagerClient)
            .setConfig(clientConfig)
            .build());

    BlockOutputStreamEntryPool blp = spy(keyOutputStream.getBlockOutputStreamEntryPool());
    BlockOutputStreamEntry ble = mock(BlockOutputStreamEntry.class);

    doThrow(IOException.class).when(ble).write(any(byte[].class),anyInt(),anyInt());
    doReturn(pipeline).when(ble).getPipeline();
    doReturn(new BlockID(1,1)).when(ble).getBlockID();
//    streamEntry.getFailedServers
    List<DatanodeDetails> dns = new ArrayList<>(3);
    for (int i = 0; i < 3; i++) {
      dns.add(
          DatanodeDetails.getFromProtoBuf(HddsProtos.DatanodeDetailsProto.newBuilder().setUuid128(
                  HddsProtos.UUID.newBuilder().setLeastSigBits(i).setMostSigBits(i)
                      .build()).setHostName("localhost").setIpAddress("1.2.3.4")
              .addPorts(HddsProtos.Port.newBuilder().setName("EC").setValue(i)
                  .build()).build())
      );
    }
    doReturn(dns).when(ble).getFailedServers();

    doReturn(ble).when(blp).allocateBlockIfNeeded();
    doReturn(4L).when(blp).getKeyLength();
    keyOutputStream.setBlockOutputStreamEntryPool(blp);

    doReturn(4)
        .when(keyOutputStream)
        .getDataWritten(any(BlockOutputStreamEntry.class),anyLong());
    return keyOutputStream;
  }

  class MockBlockOutputStreamEntryPool extends BlockOutputStreamEntryPool {

    MockBlockOutputStreamEntryPool(
        ContainerClientMetrics clientMetrics) {
      super(clientMetrics);
    }

    @Override
    protected Clock createExcludeListClock() {
//      return super.createExcludeListClock();
      return new TestClock(Instant.now(), ZoneOffset.UTC);

    }


  }

}
