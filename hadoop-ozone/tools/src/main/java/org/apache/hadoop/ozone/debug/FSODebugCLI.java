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

package org.apache.hadoop.ozone.debug;

import org.apache.hadoop.hdds.cli.SubcommandWithParent;
//import org.apache.hadoop.ozone.common.FSOBaseCLI;
import org.apache.hadoop.ozone.common.FSOBaseTool;
import org.apache.hadoop.ozone.common.FSORepairOptions;
import org.kohsuke.MetaInfServices;
import picocli.CommandLine;

/**
 * Parser for om.db file.
 */
@CommandLine.Command(
    name = "fso-tree",
    description = "Identify a disconnected FSO tree, and optionally mark " +
        "unreachable entries for deletion. OM should be " +
        "stopped while this tool is run. Information will be logged at " +
        "INFO and DEBUG levels."
)
@MetaInfServices(SubcommandWithParent.class)
public class FSODebugCLI {

  @CommandLine.ParentCommand
  private OzoneDebug parent;

  @CommandLine.Mixin
  private FSORepairOptions options = new FSORepairOptions();


//  @Override
  public Void call() throws Exception {

    try {
      // TODO case insensitive enum options.
      FSOBaseTool
          baseTool = new FSOBaseTool(options.getDbPath(), true);
      baseTool.run();
    } catch (Exception ex) {
      throw new IllegalArgumentException("FSO inspection failed: " + ex.getMessage());
    }

    if (options.getVerbose()) {
      System.out.println("FSO inspection finished. See client logs for results.");
    }

    return null;
  }

//  @Override
  public Class<?> getParentType() {
    return OzoneDebug.class;
  }
}

