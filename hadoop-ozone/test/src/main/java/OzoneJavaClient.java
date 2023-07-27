

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientException;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.shell.OzoneAddress;

import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Executors;


public class OzoneJavaClient {
    private static final Logger LOG =
        LoggerFactory.getLogger(OzoneJavaClient.class);

    private String volume;
    private String bucket;
    private String key;
    private OzoneConfiguration ozoneConfiguration;
    private OzoneAddress ozoneAddress;

    public OzoneJavaClient(String volume, String bucket, String key) throws IOException {
        this.volume = volume;
        this.bucket = bucket;
        this.key = key;
        LOG.info("constructor, volume= {}. " +
            "bucket= {}.", volume, bucket);
    }


    public void run() throws IOException, OzoneClientException {
        this.ozoneAddress = new OzoneAddress("o3://ozone1/");
        this.ozoneConfiguration = new OzoneConfiguration();
        LOG.info("Creating ozone client");

        try (OzoneClient ozoneClient = ozoneAddress.createClient(ozoneConfiguration)){

            OzoneVolume volume1 = ozoneClient.getObjectStore().getVolume(volume);
            OzoneBucket ozoneBucket = volume1.getBucket(bucket);
            OzoneKey ozkey = ozoneBucket.getKey(key);
            try (OzoneInputStream is = ozoneBucket.readKey(key)) {
                byte[] fileContent = new byte[1024];
                int charCnt = is.read(fileContent);
                LOG.warn("OzoneInputStream reading: {} char, content is: {}",
                    charCnt, new String(fileContent));
            }
          
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            LOG.warn("close java client...");
        }



    }
  
    public static void main(String[] args) throws IOException, OzoneClientException {
        System.out.println(Arrays.toString(args));
        new OzoneJavaClient(
                args[0], args[1], args[2]
            )
            .run();
    }
}
