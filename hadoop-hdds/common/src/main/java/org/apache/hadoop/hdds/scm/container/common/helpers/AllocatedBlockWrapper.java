package org.apache.hadoop.hdds.scm.container.common.helpers;

import java.util.List;

public class AllocatedBlockWrapper {

  List<AllocatedBlock> allocatedBlocks;
  boolean shouldRetryFullDNList;

  public AllocatedBlockWrapper() {

  }
  public AllocatedBlockWrapper(List<AllocatedBlock> allocatedBlocks, boolean shouldRetryFullDNList) {
    this.allocatedBlocks = allocatedBlocks;
    this.shouldRetryFullDNList = shouldRetryFullDNList;
  }

  public List<AllocatedBlock> getAllocatedBlocks() {
    return allocatedBlocks;
  }

  public void setAllocatedBlocks(
      List<AllocatedBlock> allocatedBlocks) {
    this.allocatedBlocks = allocatedBlocks;
  }

  public boolean isShouldRetryFullDNList() {
    return shouldRetryFullDNList;
  }

  public void setShouldRetryFullDNList(boolean shouldRetryFullDNList) {
    this.shouldRetryFullDNList = shouldRetryFullDNList;
  }

}
