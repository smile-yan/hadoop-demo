package cn.smileyan.hadoop.rpc;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.io.Serializable;

/**
 * @author smileyan
 */
public class RPCNameNodeServer implements ClientProtocal,Serializable {

    public static final String STUPID = "stupid";

    public static void main(String[] args) throws IOException {
        RPC.Builder builder = new RPC.Builder(new Configuration()).setInstance(new RPCNameNodeServer());
        RPC.Server server = builder.setProtocol(ClientProtocal.class).setBindAddress("localhost")
                .setPort(9123).build();
        server.start();
    }

    @Override
    public String findMetaDataByName(String filename) {
        System.out.println("正在从内存中找"+filename+"的元数据信息。");
        return "找到"+filename+"元数据信息。来自服务端数据为："+STUPID;
    }
}
