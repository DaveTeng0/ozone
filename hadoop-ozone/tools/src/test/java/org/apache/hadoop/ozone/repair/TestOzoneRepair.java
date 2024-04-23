/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.repair;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.fs.ozone.Constants.OZONE_DEFAULT_USER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.anyString;



/**
 * Tests the ozone repair command.
 */
public class TestOzoneRepair {

  private final ByteArrayOutputStream out = new ByteArrayOutputStream();
  private final ByteArrayOutputStream err = new ByteArrayOutputStream();
  private static final PrintStream OLD_OUT = System.out;
  private static final PrintStream OLD_ERR = System.err;
  private static final String DEFAULT_ENCODING = UTF_8.name();

//  @Mock
//  private Console console;

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
  void testMainWhenUserIsOzoneDefaultUser() throws Exception {
    // Arrange
    String[] argv = {};
    OzoneRepair ozoneRepair = spy(new OzoneRepair());
//    RDBRepair rdbRepair = spy(new RDBRepair());
//    SnapshotRepair snapshotRepair = spy(new SnapshotRepair());
    doReturn(OZONE_DEFAULT_USER).when(ozoneRepair).getSystemUserName();

//    CommandLine cmd = new CommandLine(ozoneRepair).addSubcommand(rdbRepair);
//        .addSubcommand(snapshotRepair);

//    String[] args =
//        new String[] {"", "--db=" + "dbPath"};
//    int exitCode = cmd.execute(args);
//    assertEquals(0, exitCode);
    // Act
    int res = ozoneRepair.execute(argv);

    // Assert
//    verify(out, times(1)).println("*****_________ or.m, user = " + OZONE_DEFAULT_USER);
//    verify(ozoneRepair, times(1)).run(argv);
//    assertEquals(0, res);

    assertThat(out.toString(DEFAULT_ENCODING)).contains("Run as user: pp" + OZONE_DEFAULT_USER);

  }

  @Test
  void testMainWhenUserIsNotOzoneDefaultUser() throws Exception {
    // Arrange
    String[] argv = {};
//    String currentUser = "non-ozone";
//    try (MockedStatic<OzoneRepair> mocked = mockStatic(OzoneRepair.class)) {

    OzoneRepair ozoneRepair = spy(new OzoneRepair());
    doReturn("test").when(ozoneRepair).getSystemUserName();
    doReturn("N").when(ozoneRepair).getConsoleReadLineWithFormat(anyString(), anyString());

//      mocked.when(() -> OzoneRepair.getSystemUserName())
//          .thenReturn(OZONE_DEFAULT_USER);

//      mocked.when(() -> OzoneRepair.getConsoleReadLineWithFormat(OZONE_DEFAULT_USER, OZONE_DEFAULT_USER))
//          .thenReturn("y");

      // Act
//      new OzoneRepair().main(argv);
    int res = ozoneRepair.execute(argv);
//      OzoneRepair.main(argv);

      // Assert
//      verify(console1, times(1)).readLine();

//      verify(console1, times(1)).readLine(anyString(), ArgumentMatchers.<Object>any());
//      verify(ozoneRepair, times(1)).run(argv);
//      mocked.verify(() -> OzoneRepair.executeOzoneRepair(any(String[].class)));
    assertEquals(1, res);
    assertThat(out.toString(DEFAULT_ENCODING)).contains("Aborting command.ll");

//    }
  }


  @Test
  void testMainWhenUserDeclinesToProceed() throws Exception {
    // Arrange
    String[] argv = {};
    OzoneRepair ozoneRepair = spy(new OzoneRepair());
    doReturn("non-ozone").when(ozoneRepair).getSystemUserName();

    doReturn("y").when(ozoneRepair).getConsoleReadLineWithFormat(anyString(), anyString());

    // Act
    ozoneRepair.execute(argv);

    // Assert
    assertThat(out.toString(DEFAULT_ENCODING)).contains("Run as user: " + "non-ozone");

//    verify(System.out, times(1)).println(
//        "ATTENTION: You are currently logged in as user '" + currentUser +
//            "'. Ozone typically runs as user '" + OZONE_DEFAULT_USER + "'." +
//            " If you proceed with this command, it may change the ownership of RocksDB " +
//            "files used by the Ozone Manager (OM)." +
//            " This ownership change could prevent OM from starting successfully." +
//            " Are you sure you want to continue (y/N)? ");
//    verify(System.out, times(1)).println("Aborting command.");
//    verify(new OzoneRepair(), never()).run(argv);
  }
}
