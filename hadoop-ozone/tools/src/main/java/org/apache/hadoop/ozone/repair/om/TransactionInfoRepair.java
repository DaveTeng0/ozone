package org.apache.hadoop.ozone.repair.om;


import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.cli.SubcommandWithParent;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.StringCodec;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;

import org.apache.hadoop.ozone.debug.RocksDBUtils;

import org.apache.hadoop.ozone.repair.RDBRepair;

import org.kohsuke.MetaInfServices;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import java.util.concurrent.Callable;

import static org.apache.hadoop.ozone.OzoneConsts.TRANSACTION_INFO_KEY;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.TRANSACTION_INFO_TABLE;


//@CommandLine.Command(
//    name = "cancelprepare",
//    description = "Cancel prepare state in the OMs.",
//    mixinStandardHelpOptions = true,
//    versionProvider = HddsVersionProvider.class
//)

/**
 * Tool to update the highest index in transactionInfoTable.
 */
@CommandLine.Command(
    name = "tr",
    description = "CLI to update the highest index in transactionInfoTable.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class

)
@MetaInfServices(SubcommandWithParent.class)

public class TransactionInfoRepair implements Callable<Void>, SubcommandWithParent  {

//  @CommandLine.ParentCommand
//  private OMAdmin parent;
//
//  @CommandLine.Option(
//      names = {"-id", "--service-id"},
//      description = "Ozone Manager Service ID",
//      required = true
//  )
//  private String omServiceId;

  @CommandLine.Spec
  private static CommandLine.Model.CommandSpec spec;

  @CommandLine.ParentCommand
  private RDBRepair parent;

  @CommandLine.Option(names = {"--highest-transaction"},
      required = true,
      description = "highest termIndex of transactionInfoTable")
  private String highestTransactionInfo;


  @Override
  public Void call() throws Exception {
    List<ColumnFamilyHandle> cfHandleList = new ArrayList<>();
    List<ColumnFamilyDescriptor> cfDescList = RocksDBUtils.getColumnFamilyDescriptors(parent.getDbPath());

    try (ManagedRocksDB db = ManagedRocksDB.open(parent.getDbPath(), cfDescList, cfHandleList)) {
      ColumnFamilyHandle transactionInfoCfh = getTransactionInfoCfh(cfHandleList);
      if (transactionInfoCfh == null) {
        System.err.println(TRANSACTION_INFO_TABLE + " is not in a column family in DB for the given path.");
        return null;
      }
      TransactionInfo transactionInfo = TransactionInfo.valueOf(highestTransactionInfo);

//      if (dryRun) {
//        System.out.println("SnapshotInfo would be updated to : " + snapshotInfo);
//      } else {
      byte[] transactionInfoBytes = TransactionInfo.getCodec().toPersistedFormat(transactionInfo);
      db.get()
          .put(transactionInfoCfh, StringCodec.get().toPersistedFormat(TRANSACTION_INFO_KEY), transactionInfoBytes);

      System.out.println("Highest Transaction Info is updated to : " +
          getTransactionInfo(db, transactionInfoCfh, TRANSACTION_INFO_KEY));
//      }
    } catch (RocksDBException exception) {
      System.err.println("Failed to update the RocksDB for the given path: " + parent.getDbPath());
      System.err.println(
          "Make sure that Ozone entity (OM, SCM or DN) is not running for the give dbPath and current host.");
      System.err.println(exception);
    } finally {
      IOUtils.closeQuietly(cfHandleList);
    }

    return null;
  }


  private ColumnFamilyHandle getTransactionInfoCfh(List<ColumnFamilyHandle> cfHandleList) throws RocksDBException {
    byte[] nameBytes = TRANSACTION_INFO_TABLE.getBytes(StandardCharsets.UTF_8);

    for (ColumnFamilyHandle cf : cfHandleList) {
      if (Arrays.equals(cf.getName(), nameBytes)) {
        return cf;
      }
    }

    return null;
  }

  private TransactionInfo getTransactionInfo(ManagedRocksDB db,
                                             ColumnFamilyHandle transactionInfoCfh,
                                             String transactionInfoLKey)
      throws IOException, RocksDBException {
    byte[] bytes = db.get().get(transactionInfoCfh, StringCodec.get().toPersistedFormat(transactionInfoLKey));
    return bytes != null ? TransactionInfo.getCodec().fromPersistedFormat(bytes) : null;
  }

  @Override
  public Class<?> getParentType() {
    return RDBRepair.class;
  }

}
