package cn.smileyan.hadoop.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * @author smileyan
 */
public class RPCHDFSClient {
    public static void main(String[] args) throws IOException {
        ClientProtocal proxy = RPC.getProxy(ClientProtocal.class, 12323,
                new InetSocketAddress("localhost", 9123),
                new Configuration());
        String msg = proxy.findMetaDataByName("/my/word.txt");
        System.out.println(msg);
        RPC.stopProxy(proxy);
    }
}
