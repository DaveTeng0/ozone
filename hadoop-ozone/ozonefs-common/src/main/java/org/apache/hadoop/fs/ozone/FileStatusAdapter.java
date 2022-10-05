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
package org.apache.hadoop.fs.ozone;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Class to hold the internal information of a FileStatus.
 * <p>
 * As FileStatus class is not compatible between 3.x and 2.x hadoop we can
 * use this adapter to hold all the required information. Hadoop 3.x FileStatus
 * information can be converted to this class, and this class can be used to
 * create hadoop 2.x FileStatus.
 * <p>
 * FileStatus (Hadoop 3.x) --> FileStatusAdapter --> FileStatus (Hadoop 2.x)
 */
public  class FileStatusAdapter extends FileStatus {

  private final long length;
  public final long diskConsumed;

  private final Path path;
  private final boolean isdir;
  private final short blockReplication;
  private final long blocksize;
  private final long modificationTime;
  private final long accessTime;
  private final short permissionV2;
  private final String owner;
  private final String group;
  private final Path symlink;
  private final BlockLocation[] blockLocations;

  @SuppressWarnings("checkstyle:ParameterNumber")
  public FileStatusAdapter(long length, long diskConsumed, Path path, boolean isdir,
      short blockReplication, long blocksize, long modificationTime,
      long accessTime, short permission, String owner,
      String group, Path symlink, BlockLocation[] locations) {
    this.length = length;
    this.diskConsumed = diskConsumed;
    this.path = path;
    this.isdir = isdir;
    this.blockReplication = blockReplication;
    this.blocksize = blocksize;
    this.modificationTime = modificationTime;
    this.accessTime = accessTime;
    this.permissionV2 = permission;
    this.owner = owner;
    this.group = group;
    this.symlink = symlink;
    this.blockLocations = locations.clone();
  }


  public Path getPath() {
    return path;
  }

 public boolean isDir(String test) {
   return isdir;
 }

  // public boolean isDirectory() {
  //   return isDirectory();
  // }

  public short getBlockReplication() {
    return blockReplication;
  }

  public long getBlocksize() {
    return blocksize;
  }

  public long getModificationTime() {
    return modificationTime;
  }

  public long getAccessTime() {
    return accessTime;
  }

  public short getPermissionV2() {
    return this.permissionV2;
  }

  public String getOwner() {
    return owner;
  }

  public String getGroup() {
    return group;
  }

  public Path getSymlink() {
    return symlink;
  }

  public long getLength() {
    return length;
  }

  public long getDiskConsumed() {
    return diskConsumed;
  }

  @SuppressFBWarnings("EI_EXPOSE_REP")
  public BlockLocation[] getBlockLocations() {
    return blockLocations;
  }

}
