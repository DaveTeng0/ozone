package org.apache.hadoop.ozone.om.helpers;

import java.util.List;

public class OmKeyLocationInfoWrapper {

  public List<OmKeyLocationInfo> getOmKeyLocationInfo() {
    return omKeyLocationInfo;
  }

  public void setOmKeyLocationInfo(
      List<OmKeyLocationInfo> omKeyLocationInfo) {
    this.omKeyLocationInfo = omKeyLocationInfo;
  }

  public boolean isShouldRetryFullDNList() {
    return shouldRetryFullDNList;
  }

  public void setShouldRetryFullDNList(boolean shouldRetryFullDNList) {
    this.shouldRetryFullDNList = shouldRetryFullDNList;
  }

  private List<OmKeyLocationInfo> omKeyLocationInfo;
  private boolean shouldRetryFullDNList;
}
