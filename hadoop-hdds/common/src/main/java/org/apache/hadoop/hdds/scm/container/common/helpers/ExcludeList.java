/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.container.common.helpers;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class contains set of dns and containers which ozone client provides
 * to be handed over to SCM when block allocation request comes.
 */
public class ExcludeList {
  private static final Logger LOG =
      LoggerFactory.getLogger(ExcludeList.class);

  private final Map<DatanodeDetails, Long> datanodes;
  private final Set<ContainerID> containerIds;
  private final Set<PipelineID> pipelineIds;
  private long expiryTime = 0;
  private java.time.Clock clock;


  public ExcludeList() {
    LOG.warn("********* aa4 ExcludeList() with expiryTime = " + expiryTime);

    datanodes = new ConcurrentHashMap<>();
    containerIds = new HashSet<>();
    pipelineIds = new HashSet<>();
    clock = Clock.system(ZoneOffset.UTC);
  }

  public ExcludeList(long autoExpiryTime, java.time.Clock clock) {
    this();
    this.expiryTime = autoExpiryTime;
    this.clock = clock;
  }

  public Set<ContainerID> getContainerIds() {
    return containerIds;
  }

  public Set<DatanodeDetails> getDatanodes() {
    Set<DatanodeDetails> dns = new HashSet<>();
//    LOG.warn("********* aa2 getDatanodes() with expiryTime = " + expiryTime);

    LOG.warn("********* aa2.5 datanodes.entrySet().size = " + datanodes.entrySet().size());

    Arrays.stream(Thread.currentThread().getStackTrace())
        .forEach(s -> System.out.println(
            "\tat " + s.getClassName() + "." + s.getMethodName() + "(" + s.getFileName() + ":" + s
                .getLineNumber() + ")"));


    if (expiryTime > 0) {
      Iterator<Map.Entry<DatanodeDetails, Long>> iterator =
          datanodes.entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<DatanodeDetails, Long> entry = iterator.next();
        Long storedExpiryTime = entry.getValue();
        if (clock.millis() > storedExpiryTime) {
          iterator.remove(); // removing
        } else {
          dns.add(entry.getKey());
        }
      }
    } else {
      dns = datanodes.keySet();
    }
    LOG.warn("********* aa2.6 getDatanodes() dns keySet size = " + datanodes.entrySet().size());

    return dns;
  }

  public void addDatanodes(Collection<DatanodeDetails> dns) {
    dns.forEach(dn -> addDatanode(dn)); // hhhhhhhhhhh write integration test for RATIS key write with datanode expiried time
  }

  public void addDatanode(DatanodeDetails dn) {
    LOG.warn("********* aaaaa5 adding dn with expiryTime = " + expiryTime);
    datanodes.put(dn, clock.millis() + expiryTime);
  }

  public void addConatinerId(ContainerID containerId) {
    containerIds.add(containerId);
  }

  public void addPipeline(PipelineID pipelineId) {
    pipelineIds.add(pipelineId);
  }

  public Set<PipelineID> getPipelineIds() {
    return pipelineIds;
  }

  public HddsProtos.ExcludeListProto getProtoBuf() {
    HddsProtos.ExcludeListProto.Builder builder =
        HddsProtos.ExcludeListProto.newBuilder();
    containerIds
        .forEach(id -> builder.addContainerIds(id.getId()));
    getDatanodes().forEach(dn -> builder.addDatanodes(dn.getUuidString()));
    pipelineIds.forEach(pipelineID -> {
      builder.addPipelineIds(pipelineID.getProtobuf());
    });
    return builder.build();
  }

  public static ExcludeList getFromProtoBuf(
      HddsProtos.ExcludeListProto excludeListProto) {
    ExcludeList excludeList = new ExcludeList();
    excludeListProto.getContainerIdsList().forEach(id -> {
      excludeList.addConatinerId(ContainerID.valueOf(id));
    });
    DatanodeDetails.Builder builder = DatanodeDetails.newBuilder();
    excludeListProto.getDatanodesList().forEach(dn -> {
      builder.setUuid(UUID.fromString(dn));
      excludeList.addDatanode(builder.build());
    });
    excludeListProto.getPipelineIdsList().forEach(pipelineID -> {
      excludeList.addPipeline(PipelineID.getFromProtobuf(pipelineID));
    });
    return excludeList;
  }

  public boolean isEmpty() {
    return getDatanodes().isEmpty() && containerIds.isEmpty() && pipelineIds
        .isEmpty();
  }

  public void clear() {
    datanodes.clear();
    containerIds.clear();
    pipelineIds.clear();
  }

  @Override
  public String toString() {
    return "ExcludeList {" +
        "datanodes = " + datanodes.keySet() +
        ", containerIds = " + containerIds +
        ", pipelineIds = " + pipelineIds +
        '}';
  }

  public long getExpiryTime() {
    return expiryTime;
  }

  public Clock getClock() {
    return clock;
  }
}
