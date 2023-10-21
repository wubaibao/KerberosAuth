package com.kerberos.authhdfs.testcode;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;

/**
 * Spark操作Kerberos认证的HDFS
 */
public class SparkOperateAuthHDFS {
    public static void main(String[] args) throws IOException {
        //进行kerberos认证
        System.setProperty("java.security.krb5.conf", "D:\\idea_space\\KerberosAuth\\KerberosAuthHDFS\\src\\main\\resources\\krb5.conf");
        String principal = "zhangsan@EXAMPLE.COM";
        String keytabPath = "D:\\idea_space\\KerberosAuth\\KerberosAuthHDFS\\src\\main\\resources\\zhangsan.keytab";
        UserGroupInformation.loginUserFromKeytab(principal, keytabPath);


        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("SparkOperateAuthHDFS");

        JavaSparkContext jsc = new JavaSparkContext(conf);

        jsc.textFile("hdfs://mycluster/wc.txt").foreach(line -> System.out.println(line));

        jsc.stop();
    }

}
