package com.kerberos.authhive.testcode;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.sql.SparkSession;
import java.io.IOException;

/**
 * Spark 读取Kerberos认证Hive的数据
 */
public class SparkReadAuthHive {
    public static void main(String[] args) throws IOException {
        //进行kerberos认证
        System.setProperty("java.security.krb5.conf", "D:\\idea_space\\KerberosAuth\\KerberosAuthHDFS\\src\\main\\resources\\krb5.conf");
        String principal = "hive/node1@EXAMPLE.COM";
        String keytabPath = "D:\\idea_space\\KerberosAuth\\KerberosAuthHive\\src\\main\\resources\\hive.service.keytab";
        UserGroupInformation.loginUserFromKeytab(principal, keytabPath);

        SparkSession spark = SparkSession.builder().appName("SparkReadAuthHive")
                .master("local")
//                .config("hive.metastore.uris", "thrift://node1:9083")
                .enableHiveSupport()
                .getOrCreate();
        spark.sql("select * from person").show();
        spark.stop();

    }
}
