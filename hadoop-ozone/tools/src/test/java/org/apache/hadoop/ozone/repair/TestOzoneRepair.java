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

//import org.jooq.meta.derby.sys.Sys;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
//import org.junit.jupiter.api.extension.ExtendWith;
//import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.MockedStatic;
//import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayOutputStream;
import java.io.Console;
import java.io.PrintStream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.fs.ozone.Constants.OZONE_DEFAULT_USER;
import static org.assertj.core.api.Assertions.assertThat;
//import static org.junit.jupiter.api.Assertions.assertEquals;
//import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.mockStatic;

/**
 * Tests the ozone repair command.
 */
public class TestOzoneRepair {

  private final ByteArrayOutputStream out = new ByteArrayOutputStream();
  private final ByteArrayOutputStream err = new ByteArrayOutputStream();
  private static final PrintStream OLD_OUT = System.out;
  private static final PrintStream OLD_ERR = System.err;
  private static final String DEFAULT_ENCODING = UTF_8.name();

  @Mock
  private Console console;

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

//  @Test
  void testMainWhenUserIsOzoneDefaultUser() throws Exception {
    // Arrange
    String[] argv = {};
//    PrintStream out = mock(PrintStream.class);
//    when(out.println(anyString())).thenReturn("")
//    OzoneRepair ozoneRepair = mock(OzoneRepair.class);
    OzoneRepair ozoneRepair = spy(new OzoneRepair());

    doReturn(OZONE_DEFAULT_USER).when(ozoneRepair).getSystemUserName();

    // Act
    ozoneRepair.main(argv);

    // Assert
//    verify(out, times(1)).println("*****_________ or.m, user = " + OZONE_DEFAULT_USER);
    verify(ozoneRepair, times(1)).run(argv);
    assertThat(out.toString(DEFAULT_ENCODING)).contains("Aborting command");

  }

  @Test
  void testMainWhenUserIsNotOzoneDefaultUser() throws Exception {
    // Arrange
    String[] argv = {};
//    String currentUser = "non-ozone";
    try (MockedStatic<OzoneRepair> mocked = mockStatic(OzoneRepair.class)) {
      mocked.when(OzoneRepair::getSystemUserName)
          .thenReturn(OZONE_DEFAULT_USER);
//      Console console1 = mock(Console.class);
//      mocked.when(() -> OzoneRepair.getConsole()).thenReturn(console1);

//      mockedSystem.when(System::console).thenReturn(console1);
      mocked.when(() -> OzoneRepair.getConsoleReadLineWithFormat(OZONE_DEFAULT_USER, OZONE_DEFAULT_USER))
          .thenReturn("y");

      //    when(System.getProperty("user.name")).thenReturn(currentUser);
//      when(console1.readLine(any())).thenReturn("y");

      // Act
//      new OzoneRepair().main(argv);
      OzoneRepair.main(argv);

      // Assert
//      verify(console1, times(1)).readLine();

//      verify(console1, times(1)).readLine(anyString(), ArgumentMatchers.<Object>any());
//      verify(new OzoneRepair(), times(1)).run(argv);
      mocked.verify(() -> OzoneRepair.executeOzoneRepair(any(String[].class)));
//      assertThat(out.toString(DEFAULT_ENCODING)).contains("Aborting command");

    }
  }


//  @Test
  void testMainWhenUserDeclinesToProceed() throws Exception {
    // Arrange
    String[] argv = {};
    String currentUser = "non-ozone";
    when(System.getProperty("user.name")).thenReturn(currentUser);
    when(console.readLine(any())).thenReturn("N");

    // Act
    OzoneRepair.main(argv);

    // Assert
    verify(System.out, times(1)).println(
        "ATTENTION: You are currently logged in as user '" + currentUser +
            "'. Ozone typically runs as user '" + OZONE_DEFAULT_USER + "'." +
            " If you proceed with this command, it may change the ownership of RocksDB " +
            "files used by the Ozone Manager (OM)." +
            " This ownership change could prevent OM from starting successfully." +
            " Are you sure you want to continue (y/N)? ");
    verify(System.out, times(1)).println("Aborting command.");
    verify(new OzoneRepair(), never()).run(argv);
  }
}
