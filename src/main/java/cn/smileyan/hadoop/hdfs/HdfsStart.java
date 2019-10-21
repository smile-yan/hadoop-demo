package cn.smileyan.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 首个测试，用来创建一个文件夹/java
 */
public class HdfsStart {
    private FileSystem fs;
    // 初始化
    public void init() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        fs = FileSystem.get(new URI("hdfs://localhost:9000"),conf);
    }

    // 创建文件夹
    public void testMkdir() throws IOException {
        boolean flag = fs.mkdirs(new Path("/java"));
        System.out.println(flag);
    }

    /**
     * 创建成功则会输出true
     * @param args
     * @throws InterruptedException
     * @throws IOException
     * @throws URISyntaxException
     */
    public static void main(String[] args) throws InterruptedException, IOException, URISyntaxException {
        HdfsStart hdfsStart = new HdfsStart();
        hdfsStart.init();
        hdfsStart.testMkdir();
    }

}
