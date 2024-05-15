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

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.debug.DBScanner;
import org.apache.hadoop.ozone.debug.RDBParser;
import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.ozone.repair.RDBRepair;
import org.apache.hadoop.ozone.repair.TransactionInfoRepair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.OzoneConsts.OM_DB_NAME;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test Ozone Repair shell.
 */
public class TestOzoneRepairShell {

  private final ByteArrayOutputStream out = new ByteArrayOutputStream();
  private final ByteArrayOutputStream err = new ByteArrayOutputStream();
  private static final PrintStream OLD_OUT = System.out;
  private static final PrintStream OLD_ERR = System.err;
  private static final String DEFAULT_ENCODING = UTF_8.name();
  private static MiniOzoneCluster cluster = null;
  private static OzoneConfiguration conf = null;

  private static final String TRANSACTION_INFO_TABLE_TERM_INDEX_PATTERN = "([0-9]+#[0-9]+)";

  @BeforeAll
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    cluster = MiniOzoneCluster.newBuilder(conf).build();
    cluster.waitForClusterToBeReady();
  }

  @BeforeEach
  public void setup() throws Exception {
    System.setOut(new PrintStream(out, false, DEFAULT_ENCODING));
    System.setErr(new PrintStream(err, false, DEFAULT_ENCODING));
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

  @Test
  public void testUpdateTransactionInfoTable() throws Exception {
    StringWriter stdout = new StringWriter();
    PrintWriter pstdout = new PrintWriter(stdout);
    CommandLine cmd = new CommandLine(new RDBRepair()).addSubcommand(new TransactionInfoRepair());
//        .setOut(pstdout);
    String dbPath = OMStorage.getOmDbDir(conf) + OM_KEY_PREFIX + OM_DB_NAME;

    cluster.getOzoneManager().stop();

    String cmdOut = scanTransactionInfoTable(dbPath);
    String originalHighestTermIndex = parseScanOutput(cmdOut);

    String testTerm = "1111";
    String testIndex = "1111";
    String[] args =
        new String[] {"--db=" + dbPath, "transaction", "--highest-transaction", testTerm + "#" + testIndex};
    int exitCode = cmd.execute(args);
    assertEquals(0, exitCode);
    assertThat(stdout.toString()).contains("The original highest transaction Info was " + originalHighestTermIndex);
    assertThat(stdout.toString()).contains(
        String.format("The highest transaction info has been updated to: (t:%s, i:%s)",
        testTerm, testIndex));

    String cmdOut2 = scanTransactionInfoTable(dbPath);
    assertThat(cmdOut2).contains(testTerm + "#" + testIndex);
    cluster.getOzoneManager().restart();
  }

  private String scanTransactionInfoTable(String dbPath) {
    StringWriter stdout = new StringWriter();
    PrintWriter pstdout = new PrintWriter(stdout);
    CommandLine cmdDBScanner = new CommandLine(new RDBParser()).addSubcommand(new DBScanner());
//        .setOut(pstdout);
    String[] argsDBScanner =
        new String[] {"--db=" + dbPath, "scan", "--column_family", "transactionInfoTable"};
    cmdDBScanner.execute(argsDBScanner);
    return out.toString();
  }

  private String parseScanOutput(String output) {
    Pattern pattern = Pattern.compile(TRANSACTION_INFO_TABLE_TERM_INDEX_PATTERN);
    Matcher matcher = pattern.matcher(output);
    if (matcher.find()) {
      String[] termIndex = matcher.group(1).split("#");
      return String.format("(t:%s, i:%s)", termIndex[0], termIndex[1]);
    }
    return "dummyTermIndex";
  }

}
