package com.kerberos.authhdfs.testcode;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;

/**
 * Flink 读取Kerberos认证的HDFS文件
 */
public class FlinkOperateAuthHDFS {
    public static void main(String[] args) throws Exception {
        //进行kerberos认证
        System.setProperty("java.security.krb5.conf", "D:\\idea_space\\KerberosAuth\\KerberosAuthHDFS\\src\\main\\resources\\krb5.conf");
        String principal = "zhangsan@EXAMPLE.COM";
        String keytabPath = "D:\\idea_space\\KerberosAuth\\KerberosAuthHDFS\\src\\main\\resources\\zhangsan.keytab";
        UserGroupInformation.loginUserFromKeytab(principal, keytabPath);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FileSource<String> fileSource = FileSource.forRecordStreamFormat(
                new TextLineInputFormat(),
                new Path("hdfs://mycluster/wc.txt")).build();

        DataStreamSource<String> dataStream = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "file-source");

        dataStream.print();

        env.execute();
    }
}
