package cn.smileyan.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Scanner;

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
    public void mkdir(String dir) throws IOException {
        boolean flag = fs.mkdirs(new Path(dir));
        if(flag) {
            System.out.println("Successfully make direction " + dir );
        } else {
            System.out.println("Failed to mkdir " + dir);
        }
    }

    /**
     *
     * @param msg
     * @param pathAndFileName
     * @throws IOException
     */
    public void create(String pathAndFileName, String msg) throws IOException {
        FSDataOutputStream fsDataOutputStream = fs.create(new Path(pathAndFileName));
        fsDataOutputStream.writeChars(msg);
        fsDataOutputStream.close();
        System.out.println("Created file "+pathAndFileName +" and its message is "+msg);
    }

    /**
     * 文本后添加
     * @param msg
     * @param pathAndFileName
     * @throws IOException
     */
    public void append(String pathAndFileName,String msg) throws IOException {
        FSDataOutputStream fsDataOutputStream = fs.append(new Path(pathAndFileName));
        fsDataOutputStream.writeChars(msg);
        // 必须要close时才生效
        fsDataOutputStream.close();
        System.out.println("Appended message:"+msg);
    }

    /**
     * 读文件
     * @param pathAndFileName
     * @throws IOException
     */
    public void read(String pathAndFileName) throws IOException {
        FSDataInputStream fsDataOutputStream = fs.open(new Path(pathAndFileName));
        byte[] bytes = new byte[1024];
        int read = fsDataOutputStream.read(bytes);
        System.out.println("File("+pathAndFileName+"):  "+new String(bytes,0,read));
    }

    /**
     * 重命名
     * @param oldPathAndFileName
     * @param newPathAndFileName
     * @return
     * @throws IOException
     */
    public void rename(String oldPathAndFileName, String newPathAndFileName) throws IOException {
        boolean flag = fs.rename(new Path(oldPathAndFileName), new Path(newPathAndFileName));
        if(flag) {
            System.out.println("Successfully rename " + oldPathAndFileName +" to "+ newPathAndFileName );
        } else {
            System.out.println("Failed to  rename " + oldPathAndFileName +" to "+ newPathAndFileName );
        }
    }

    /**
     * delete on exit
     * @param pathAndFileName
     * @return
     * @throws IOException
     */
    public void deleteFile(String pathAndFileName) throws IOException {
        boolean flag = fs.delete(new Path(pathAndFileName),true);
        if(flag) {
            System.out.println("Deleted "+pathAndFileName);
        } else {
            System.out.println("Failed to delete "+pathAndFileName);
        }
    }

    /**
     * 注意 listLocatedStatus 方法和 listFiles 方法的不同。
     * listLocatedStatus 单纯地列举出当前目录下所有文件和目录，包含空目录
     * listFiles 递归列出所有文件，不包含空目录
     * 列出所有
     * @param path
     * @throws IOException
     */
    public void list(String path) throws IOException {
        RemoteIterator<LocatedFileStatus> list =
                fs.listLocatedStatus(new Path(path));
        while(list.hasNext()) {
            LocatedFileStatus next = list.next();
            Path path1 = next.getPath();
            System.out.println(path1);
        }
    }
    public static void main(String[] args) throws InterruptedException, IOException, URISyntaxException {
        HdfsBase hdfsBase = new HdfsBase();
        hdfsBase.init();
        hdfsBase.help();
        Scanner scanner = new Scanner(System.in);
        boolean exit = false;
        while(exit == false) {
            System.out.print("> ");
            String s = scanner.nextLine();
            String[] words = s.split(" ");
            try {
                switch (words[0]) {
                    case "help":
                        hdfsBase.help();
                        break;
                    case "bye":
                        exit = true;
                        System.out.println("Thanks for using STUPID BOY Tool ! Bye ");
                        break;
                    case "mkdir":
                        if (hdfsBase.check(words, 2) == false) {
                            break;
                        }
                        hdfsBase.mkdir(words[1]);
                        break;
                    case "del":
                        if (hdfsBase.check(words, 2) == false) {
                            break;
                        }
                        hdfsBase.deleteFile(words[1]);
                        break;
                    case "mv":
                        if (hdfsBase.check(words, 3) == false) {
                            break;
                        }
                        hdfsBase.rename(words[1], words[2]);
                        break;
                    case "read":
                        if (hdfsBase.check(words, 2) == false) {
                            break;
                        }
                        hdfsBase.read(words[1]);
                        break;
                    case "append":
                        if (hdfsBase.check(words, 3) == false) {
                            break;
                        }
                        hdfsBase.append(words[1], words[2]);
                        break;
                    case "add":
                        if (hdfsBase.check(words, 3) == false) {
                            break;
                        }
                        hdfsBase.create(words[1], words[2]);
                        break;
                    case "ls":
                        if (hdfsBase.check(words, 2) == false) {
                            break;
                        }
                        hdfsBase.list(words[1]);
                        break;
                    default:
                        break;
                }
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
    }

    /**
     *
     */
    public void help() {
        System.out.println("Welcome to use STUPID BOY Tool !");
        System.out.println("\t1. You could input `mkdir /path` to create a direction.");
        System.out.println("\t2. You could input `del /pathAndFile` to delete an existent file.");
        System.out.println("\t3. You could input `ls /path` to show files or directions in this path.");
        System.out.println("\t4. You could input `mv /oldPathAndFileName /newPathAndFileName` to rename one file or move this file to another direction.");
        System.out.println("\t5. You could input `read /pathAndFileName` to read one file.");
        System.out.println("\t6. You could input `append /pathAndFileName  YourMessage` to append message in an existent file.");
        System.out.println("\t7. You could input `add /pathAndFileName YourMessage` to add one file.");
        System.out.println("\t8. You could input `bye` to close this program.");
        System.out.println("\t9. You could input `help` to know how to use this STUPID BOY Tool.");
    }

    /**
     * 检查输入指令是否有误
     * @param words
     * @param length
     * @return
     */
    public boolean check(String[] words,int length) {
        if(words.length != length) {
            System.out.println("ERROR! When you input "+words);
            return false;
        }
        return true;
    }
}
