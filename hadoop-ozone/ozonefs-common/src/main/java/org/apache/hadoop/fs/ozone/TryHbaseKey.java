package org.apache.hadoop.fs.ozone;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.hadoop.ozone.OzoneConsts.*;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;

public class TryHbaseKey {

  public static void main(String[] args) {
    return;
  }

  /*
  public static void main(String[] args) throws Exception{

    writeTestKey();

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

  public static void writeTestKey() throws Exception{
    OzoneConfiguration CONF = new OzoneConfiguration();

    // Set the fs.defaultFS
    final String rootPath = String.format("%s://%s/",
        OZONE_OFS_URI_SCHEME, "ozone1");
    CONF.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);

    final String dir = OZONE_ROOT + "vol1"
        + OZONE_URI_DELIMITER + "buck1";

    try (FileSystem fs = FileSystem.get(CONF)) {

      for (int i = 0; i < 10; i++) {
        final Path file = new Path(dir, "file" + i);
        StreamWithLength out =
            new StreamWithLength(fs.create(file,true));
        int dataSize = 10;
        final byte[] data = new byte[dataSize];
        ThreadLocalRandom.current().nextBytes(data);
        out.writeAndHsync(data);
//        out.close();
      }

    }

  }
*/
}

/*
class StreamWithLength implements Closeable {
  private final FSDataOutputStream out;
  private long length = 0;

  StreamWithLength(FSDataOutputStream out) {
    this.out = out;
  }

  long getLength() {
    return length;
  }

  void writeAndHsync(byte[] data) throws IOException {
    out.write(data);
    out.hsync();
    length += data.length;
  }

  @Override
  public void close() throws IOException {
    out.close();
  }
}
*/
