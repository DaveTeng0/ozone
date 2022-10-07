/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.om.helpers;

import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType.EC;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType.RATIS;

/**
 * Helper class to calculate quota related usage.
 */
public final class QuotaUtil {

  private QuotaUtil() {
  };

  static final Logger LOG =
          LoggerFactory.getLogger(QuotaUtil.class);

  /**
   * From the used space and replicationConfig, calculate the expected
   * replicated size of the data.
   * @param dataSize The number of bytes of data stored
   * @param repConfig The replicationConfig used to store the data
   * @return Number of bytes required to store the dataSize with replication
   */
  public static long getReplicatedSize(
      long dataSize, ReplicationConfig repConfig) {
    if (repConfig.getReplicationType() == RATIS) {
      return dataSize * ((RatisReplicationConfig)repConfig)
          .getReplicationFactor().getNumber();
    } else if (repConfig.getReplicationType() == EC) {
      ECReplicationConfig rc = (ECReplicationConfig)repConfig;
      int dataStripeSize = rc.getData() * rc.getEcChunkSize();
      LOG.info("@________W________@");
      LOG.info(String.format("rc.getData() = %d, rc.getEcChunkSize() = %d, dataStripeSize = %d",
              rc.getData(),rc.getEcChunkSize(),  dataStripeSize));
      long fullStripes = dataSize / dataStripeSize;
      LOG.info(String.format("dataSize = %d, dataStripeSize = %d, fullStripes = %d", dataSize, dataStripeSize, fullStripes));
      long partialFirstChunk =
          Math.min(rc.getEcChunkSize(), dataSize % dataStripeSize);
      LOG.info(String.format("partialFirstChunk = %d, Math.min( %d, %d ---  %d) => %d", partialFirstChunk, rc.getEcChunkSize(),
              dataSize, dataStripeSize, dataSize % dataStripeSize));
      long replicationOverhead =
          fullStripes * rc.getParity() * rc.getEcChunkSize()
              + partialFirstChunk * rc.getParity();

      LOG.info(String.format("replicationOverhead = %d, %d * %d * %d " +
              " + %d * %d",  replicationOverhead, fullStripes , rc.getParity() , rc.getEcChunkSize(),
              partialFirstChunk , rc.getParity()
              ));
      LOG.info("return " + String.format("%d + %d", dataSize, replicationOverhead));
      return dataSize + replicationOverhead;
    } else {
      return dataSize;
    }
  }

}
