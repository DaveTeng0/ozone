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

import org.junit.jupiter.api.Test;


/**
 * SnapshotRepair test cases.
 */
public class TestSnapshotRepair {


  @Test
  void testMainWhenUserIsNotOzoneDefaultUser() throws Exception {
    // Arrange
    String[] argv = {};
//    String currentUser = "non-ozone";
//    try (MockedStatic<OzoneRepair> mocked = mockStatic(OzoneRepair.class)) {

//    OzoneRepair ozoneRepair = spy(new OzoneRepair());
//    doReturn(OZONE_DEFAULT_USER).when(ozoneRepair).getSystemUserName();
//    doReturn("y").when(ozoneRepair).getConsoleReadLineWithFormat(OZONE_DEFAULT_USER, OZONE_DEFAULT_USER);

//      mocked.when(() -> OzoneRepair.getSystemUserName())
//          .thenReturn(OZONE_DEFAULT_USER);
//
//      mocked.when(() -> OzoneRepair.getConsoleReadLineWithFormat(OZONE_DEFAULT_USER, OZONE_DEFAULT_USER))
//          .thenReturn("y");

      // Act
//      new OzoneRepair().main(argv);
//    ozoneRepair.execute(argv);
    OzoneRepair.main(argv);

      // Assert
//      verify(console1, times(1)).readLine();

//      verify(console1, times(1)).readLine(anyString(), ArgumentMatchers.<Object>any());
//      verify(ozoneRepair, times(1)).run(argv);
//      mocked.verify(() -> OzoneRepair.executeOzoneRepair(any(String[].class)));
//      assertThat(out.toString(DEFAULT_ENCODING)).contains("");

//    }
  }

}
