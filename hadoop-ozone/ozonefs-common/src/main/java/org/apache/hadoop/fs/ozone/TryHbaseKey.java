package org.apache.hadoop.ozone.om;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;

import java.io.File;

public class TryHbaseKey {

  public static void main(String[] args) {
    String fileName = "demo1.txt";
    try {
      File file = new File(fileName);
      file.createNewFile();
    }catch (Exception e) {
      e.printStackTrace();
//      LOG.warn("********** 1 : ", e.getCause());
      System.out.println("***** 1 " + e.getMessage());
    }

    OzoneConfiguration conf = new OzoneConfiguration();
//    String disableCache = String.format("fs.%s.impl.disable.cache",
//        OzoneConsts.OZONE_OFS_URI_SCHEME);
//    conf.setBoolean(disableCache, true);
    FileSystem fs = new BasicOzoneFileSystem();
    try {
      fs = FileSystem.get(conf);
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println("********** 2 : " + e.getMessage());
    }

    Path path1 = new Path(fileName);
    try (FSDataInputStream stream = fs.open(path1)) {
      byte[] buffer = new byte[1024];
      stream.readFully(0,buffer);
    }catch (Exception e) {
      e.printStackTrace();
      System.out.println("********** 3 : " + e.getMessage());

    }
  }

}
