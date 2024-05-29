package org.apache.hadoop.ozone.common;

import picocli.CommandLine;

/**
 * FSORepairOptions.
 */
public class FSORepairOptions {
  @CommandLine.Option(names = {"--db"},
      required = true,
      description = "Path to OM RocksDB")
  private String dbPath;

  @CommandLine.Option(names = {"--verbose"},
      description = "More verbose output. ")
  private boolean verbose;

  public String getDbPath() {
    return dbPath;
  }

  public boolean getVerbose() {
    return verbose;
  }
}
