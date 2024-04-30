/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license
 * agreements. See the NOTICE file distributed with this work for additional
 * information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache
 * License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the
 * License. You may obtain a
 * copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software
 * distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.repair;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.cli.SubcommandWithParent;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.StringCodec;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.ozone.debug.RocksDBUtils;
import org.kohsuke.MetaInfServices;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import static org.apache.hadoop.ozone.OzoneConsts.TRANSACTION_INFO_KEY;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.TRANSACTION_INFO_TABLE;


/**
 * Tool to update the highest term-index in transactionInfoTable.
 */
@CommandLine.Command(
    name = "transaction",
    description = "CLI to update the highest index in transactionInfoTable.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class
)
@MetaInfServices(SubcommandWithParent.class)
public class TransactionInfoRepair
    implements Callable<Void>, SubcommandWithParent  {

  protected static final Logger LOG = LoggerFactory.getLogger(TransactionInfoRepair.class);

  @CommandLine.Spec
  private static CommandLine.Model.CommandSpec spec;

  @CommandLine.ParentCommand
  private RDBRepair parent;

  @CommandLine.Option(names = {"--highest-transaction"},
      required = true,
      description = "Highest termIndex of transactionInfoTable. The input format is: {term}#{index}.")
  private String highestTransactionTermIndex;


  protected void setHighestTransactionTermIndex(
      String highestTransactionTermIndex) {
    this.highestTransactionTermIndex = highestTransactionTermIndex;
  }

  @Override
  public Void call() throws Exception {
    List<ColumnFamilyHandle> cfHandleList = new ArrayList<>();
    String dbPath = getParent().getDbPath();
    List<ColumnFamilyDescriptor> cfDescList = RocksDBUtils.getColumnFamilyDescriptors(
        dbPath);

    try (ManagedRocksDB db = ManagedRocksDB.open(dbPath, cfDescList, cfHandleList)) {
      ColumnFamilyHandle transactionInfoCfh = RocksDBUtils.getColumnFamilyHandle(TRANSACTION_INFO_TABLE, cfHandleList);
      if (transactionInfoCfh == null) {
        err().println(TRANSACTION_INFO_TABLE + " is not in a column family in DB for the given path.");
        return null;
      }
      TransactionInfo originalTransactionInfo =
          RocksDBUtils.getValue(db, transactionInfoCfh, TRANSACTION_INFO_KEY, TransactionInfo.getCodec());

      out().println("The original highest transaction Info was " + originalTransactionInfo.getTermIndex());

      TransactionInfo transactionInfo = TransactionInfo.valueOf(
          highestTransactionTermIndex);

      byte[] transactionInfoBytes = TransactionInfo.getCodec().toPersistedFormat(transactionInfo);
      db.get()
          .put(transactionInfoCfh, StringCodec.get().toPersistedFormat(TRANSACTION_INFO_KEY), transactionInfoBytes);

      out().println("The highest transaction info has been updated to: " +
          RocksDBUtils.getValue(db, transactionInfoCfh, TRANSACTION_INFO_KEY,
              TransactionInfo.getCodec()).getTermIndex());
    } catch (RocksDBException exception) {
      err().println("Failed to update the RocksDB for the given path: " + dbPath);
      err().println(
          "Make sure that Ozone entity (OM, SCM or DN) is not running for the give database path and current host.");
      LOG.error(exception.toString());
    } finally {
      IOUtils.closeQuietly(cfHandleList);
    }

    return null;
  }

  protected RDBRepair getParent() {
    return parent;
  }

  @Override
  public Class<?> getParentType() {
    return RDBRepair.class;
  }

  private static PrintWriter out() {
    return spec.commandLine().getOut();
  }

  private static PrintWriter err() {
    return spec.commandLine().getErr();
  }

}