package cn.smileyan.hadoop.rpc;

/**
 * 协议接口
 */
public interface ClientProtocal {
    long versionID = 12323L;
    String findMetaDataByName(String filename);
}
