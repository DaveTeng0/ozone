/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.shell;

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.kms.KMSClientProvider;
import org.apache.hadoop.crypto.key.kms.server.MiniKMS;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.ozone.OzoneFsShell;
import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.cli.OzoneAdmin;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.OFSPath;
import org.apache.hadoop.ozone.client.*;
import org.apache.hadoop.ozone.client.io.ECKeyOutputStream;
import org.apache.hadoop.ozone.client.io.KeyOutputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.TrashPolicyOzone;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ToolRunner;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.*;

import java.io.*;
import java.net.URI;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY;
import static org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY;
import static org.apache.hadoop.fs.FileSystem.TRASH_PREFIX;
import static org.apache.hadoop.hdds.HddsConfigKeys.*;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.*;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OFS_URI_SCHEME;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.*;
import static org.apache.hadoop.ozone.om.helpers.BucketLayout.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

/**
 * This class tests Ozone sh shell command.
 * Inspired by TestS3Shell
 */
@Timeout(300)
public class TestOzoneShellHA_test {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestOzoneShellHA_test.class);

  private static final String DEFAULT_ENCODING = UTF_8.name();

  private static File baseDir;
  private static File testFile;
  private static String testFilePathString;
  private static MiniOzoneHAClusterImpl cluster = null;
  private static File testDir;
  private static MiniKMS miniKMS;
  private static OzoneClient client;
  private OzoneShell ozoneShell = null;
  private OzoneAdmin ozoneAdminShell = null;

  private final ByteArrayOutputStream out = new ByteArrayOutputStream();
  private final ByteArrayOutputStream err = new ByteArrayOutputStream();
  private static final PrintStream OLD_OUT = System.out;
  private static final PrintStream OLD_ERR = System.err;

  private static String omServiceId;
  private static int numOfOMs;

  @Test
  public void t() throws Exception {

    List<OzoneManager> ls = cluster.getOzoneManagersList();
//    List<String> ls2 = new ArrayList<>();
    StringBuilder sb = new StringBuilder();
    for(OzoneManager om : ls) {
      sb.append(om.getOmRpcServerAddr().toString()).append(",");
    }
    sb.deleteCharAt(sb.length() - 1);

    String[] args = new String[] {"ratis", "group-test", "info", "-peers", sb.toString()};
    execute(ozoneAdminShell, args);
    assertEquals(out.toString(DEFAULT_ENCODING), "hello!");

  }


  /**
   * Create a MiniOzoneCluster for testing with using distributed Ozone
   * handler type.
   *
   * @throws Exception
   */
  @BeforeAll
  public static void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    startKMS();
    startCluster(conf);
  }

  protected static void startKMS() throws Exception {
    testDir = GenericTestUtils.getTestDir(
        TestOzoneShellHA_test.class.getSimpleName());
    File kmsDir = new File(testDir, UUID.randomUUID().toString());
    assertTrue(kmsDir.mkdirs());
    MiniKMS.Builder miniKMSBuilder = new MiniKMS.Builder();
    miniKMS = miniKMSBuilder.setKmsConfDir(kmsDir).build();
    miniKMS.start();
  }

  protected static void startCluster(OzoneConfiguration conf) throws Exception {
    String path = GenericTestUtils.getTempPath(
        TestOzoneShellHA_test.class.getSimpleName());
    baseDir = new File(path);
    baseDir.mkdirs();

    testFilePathString = path + OZONE_URI_DELIMITER + "testFile";
    testFile = new File(testFilePathString);
    testFile.getParentFile().mkdirs();
    testFile.createNewFile();

    // Init HA cluster
    omServiceId = "om-service-test1";
    numOfOMs = 3;
    final int numDNs = 3;
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_KEY_PROVIDER_PATH,
        getKeyProviderURI(miniKMS));
    conf.setBoolean(OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS, true);

    //copy from TestOzoneDebugShell
    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL,
        100, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(HDDS_HEARTBEAT_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(HDDS_PIPELINE_REPORT_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(HDDS_COMMAND_STATUS_REPORT_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(HDDS_CONTAINER_REPORT_INTERVAL, 1, SECONDS);
    LOG.warn("****________ tos.sc, ==> " + conf.get(HDDS_GRPC_TLS_ENABLED));

    ///////
    conf.setBoolean(HDDS_GRPC_TLS_ENABLED, true);
    conf.setBoolean(OZONE_SECURITY_ENABLED_KEY, true);

    //////
    MiniOzoneHAClusterImpl.Builder builder = MiniOzoneCluster.newHABuilder(conf);
    builder.setOMServiceId(omServiceId)
        .setNumOfOzoneManagers(numOfOMs)
        .setNumDatanodes(numDNs);
    cluster = builder.build();
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();
  }

  /**
   * shutdown MiniOzoneCluster.
   */
  @AfterAll
  public static void shutdown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }

    if (miniKMS != null) {
      miniKMS.stop();
    }

    if (baseDir != null) {
      FileUtil.fullyDelete(baseDir, true);
    }

    if (testDir != null) {
      FileUtil.fullyDelete(testDir, true);
    }
  }

  @BeforeEach
  public void setup() throws UnsupportedEncodingException {
    ozoneShell = new OzoneShell();
    ozoneAdminShell = new OzoneAdmin();
//    System.setOut(new PrintStream(out, false, DEFAULT_ENCODING));
//    System.setErr(new PrintStream(err, false, DEFAULT_ENCODING));
  }

  @AfterEach
  public void reset() {
    // reset stream after each unit test
    out.reset();
    err.reset();

    // restore system streams
    System.setOut(OLD_OUT);
    System.setErr(OLD_ERR);
  }

  private static String getKeyProviderURI(MiniKMS kms) {
    return KMSClientProvider.SCHEME_NAME + "://" +
        kms.getKMSUrl().toExternalForm().replace("://", "@");
  }


  private void execute(GenericCli shell, String[] args) {
    LOG.info("Executing OzoneShell command with args {}", Arrays.asList(args));
    CommandLine cmd = shell.getCmd();

    IExceptionHandler2<List<Object>> exceptionHandler =
        new IExceptionHandler2<List<Object>>() {
          @Override
          public List<Object> handleParseException(ParameterException ex,
              String[] args) {
            throw ex;
          }

          @Override
          public List<Object> handleExecutionException(ExecutionException ex,
              ParseResult parseRes) {
            throw ex;
          }
        };

    // Since there is no elegant way to pass Ozone config to the shell,
    // the idea is to use 'set' to place those OM HA configs.
    String[] argsWithHAConf = getHASetConfStrings(args);

    cmd.parseWithHandlers(new RunLast(), exceptionHandler, argsWithHAConf);
  }

  /**
   * Execute command, assert exception message and returns true if error
   * was thrown.
   */
  private void executeWithError(OzoneShell shell, String[] args,
      String expectedError) {
    if (Strings.isNullOrEmpty(expectedError)) {
      execute(shell, args);
    } else {
      Exception ex = assertThrows(Exception.class, () -> execute(shell, args));
      if (!Strings.isNullOrEmpty(expectedError)) {
        Throwable exceptionToCheck = ex;
        if (exceptionToCheck.getCause() != null) {
          exceptionToCheck = exceptionToCheck.getCause();
        }
        assertThat(exceptionToCheck.getMessage()).contains(expectedError);
      }
    }
  }

  /**
   * @return the leader OM's Node ID in the MiniOzoneHACluster.
   */
  private String getLeaderOMNodeId() {
    OzoneManager omLeader = cluster.getOMLeader();
    assertNotNull(omLeader, "There should be a leader OM at this point.");
    return omLeader.getOMNodeId();
  }

  private String getSetConfStringFromConf(String key) {
    return String.format("--set=%s=%s", key, cluster.getConf().get(key));
  }

  private String generateSetConfString(String key, String value) {
    return String.format("--set=%s=%s", key, value);
  }

  /**
   * Helper function to get a String array to be fed into OzoneShell.
   * @param numOfArgs Additional number of arguments after the HA conf string,
   *                  this translates into the number of empty array elements
   *                  after the HA conf string.
   * @return String array.
   */
  private String[] getHASetConfStrings(int numOfArgs) {
    assert (numOfArgs >= 0);
    String[] res = new String[1 + 1 + numOfOMs + numOfArgs];
    final int indexOmServiceIds = 0;
    final int indexOmNodes = 1;
    final int indexOmAddressStart = 2;

    res[indexOmServiceIds] = getSetConfStringFromConf(
        OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY);

    String omNodesKey = ConfUtils.addKeySuffixes(
        OMConfigKeys.OZONE_OM_NODES_KEY, omServiceId);
    String omNodesVal = cluster.getConf().get(omNodesKey);
    res[indexOmNodes] = generateSetConfString(omNodesKey, omNodesVal);

    String[] omNodesArr = omNodesVal.split(",");
    // Sanity check
    assert (omNodesArr.length == numOfOMs);
    for (int i = 0; i < numOfOMs; i++) {
      res[indexOmAddressStart + i] =
          getSetConfStringFromConf(ConfUtils.addKeySuffixes(
              OMConfigKeys.OZONE_OM_ADDRESS_KEY, omServiceId, omNodesArr[i]));
    }

    return res;
  }

  /**
   * Helper function to create a new set of arguments that contains HA configs.
   * @param existingArgs Existing arguments to be fed into OzoneShell command.
   * @return String array.
   */
  private String[] getHASetConfStrings(String[] existingArgs) {
    // Get a String array populated with HA configs first
    String[] res = getHASetConfStrings(existingArgs.length);

    int indexCopyStart = res.length - existingArgs.length;
    // Then copy the existing args to the returned String array
    for (int i = 0; i < existingArgs.length; i++) {
      res[indexCopyStart + i] = existingArgs[i];
    }
    return res;
  }

  /**
   * Helper function to generate keys for testing shell command of keys.
   */
  private void generateKeys(String volumeName, String bucketName,
                            String bucketLayout) {
    String[] args = new String[] {
        "volume", "create", "o3://" + omServiceId + volumeName};
    execute(ozoneShell, args);

    args = (Strings.isNullOrEmpty(bucketLayout)) ?
            new String[] {"bucket", "create", "o3://" + omServiceId +
                    volumeName + bucketName } :
            new String[] {"bucket", "create", "o3://" + omServiceId +
                    volumeName + bucketName, "--layout", bucketLayout};
    execute(ozoneShell, args);

    String keyName = volumeName + bucketName + OZONE_URI_DELIMITER + "key";
    for (int i = 0; i < 100; i++) {
      args = new String[] {
          "key", "put", "o3://" + omServiceId + keyName + i,
          testFile.getPath()};
      execute(ozoneShell, args);
    }
  }

  /**
   * Helper function to get nums of keys from info of listing command.
   */
  private int getNumOfKeys() throws UnsupportedEncodingException {
    return out.toString(DEFAULT_ENCODING).split("key").length - 1;
  }

  /**
   * Helper function to generate buckets for testing shell command of buckets.
   */
  private void generateBuckets(String volumeName, int numOfBuckets) {
    String[] args = new String[] {
        "volume", "create", "o3://" + omServiceId + volumeName};
    execute(ozoneShell, args);

    String bucketName =
        volumeName + OZONE_URI_DELIMITER + "testbucket";
    for (int i = 0; i < numOfBuckets; i++) {
      args = new String[] {
          "bucket", "create", "o3://" + omServiceId + bucketName + i};
      execute(ozoneShell, args);
    }
  }

  /**
   * Helper function to get nums of buckets from info of listing command.
   */
  private int getNumOfBuckets(String bucketPrefix)
      throws UnsupportedEncodingException {
    return out.toString(DEFAULT_ENCODING).split(bucketPrefix).length - 1;
  }

  /**
   * Parse output into ArrayList with Gson.
   * @return ArrayList
   */
  private ArrayList<LinkedTreeMap<String, String>> parseOutputIntoArrayList()
      throws UnsupportedEncodingException {
    return new Gson().fromJson(out.toString(DEFAULT_ENCODING), ArrayList.class);
  }




}
