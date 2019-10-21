package cn.smileyan.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsBase {
    private FileSystem fs;

    /**
     *
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    public void init() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        fs = FileSystem.get(new URI("hdfs://localhost:9000"),conf);
    }

    /**
     *
     * @param dir
     * @return
     * @throws IOException
     */
    public boolean mkdir(String dir) throws IOException {
        return fs.mkdirs(new Path("/java"));
    }

    /**
     *
     * @param msg
     * @param pathAndFileName
     * @throws IOException
     */
    public void create(String msg, String pathAndFileName) throws IOException {
        FSDataOutputStream fsDataOutputStream = fs.create(new Path(pathAndFileName));
        fsDataOutputStream.writeChars(msg);
    }

    /**
     * 文本后添加
     * @param msg
     * @param pathAndFileName
     * @throws IOException
     */
    public void append(String msg,String pathAndFileName) throws IOException {
        FSDataOutputStream fsDataOutputStream = fs.append(new Path(pathAndFileName));
        fsDataOutputStream.writeChars(msg);
    }

    /**
     * 读文件
     * @param pathAndFileName
     * @throws IOException
     */
    public void read(String pathAndFileName) throws IOException {
        FSDataInputStream fsDataOutputStream = fs.open(new Path(pathAndFileName));
        byte[] bytes = new byte[512];
        int read = fsDataOutputStream.read(bytes);
        System.out.println("File("+pathAndFileName+"):"+new String(bytes,0,read));
    }

    /**
     * 重命名
     * @param oldPathAndFileName
     * @param newPathAndFileName
     * @return
     * @throws IOException
     */
    public boolean rename(String oldPathAndFileName, String newPathAndFileName) throws IOException {
        return fs.rename(new Path(oldPathAndFileName), new Path(newPathAndFileName));
    }

    public static void main(String[] args) throws InterruptedException, IOException, URISyntaxException {
        HdfsBase hdfsBase = new HdfsBase();
        hdfsBase.init();
        boolean flag = hdfsBase.rename("/java/stupid.txt","/java/veryStupid.txt");
        System.out.println("renamed : "+flag);
    }
}
