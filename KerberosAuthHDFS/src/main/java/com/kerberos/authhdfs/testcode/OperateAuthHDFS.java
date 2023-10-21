package com.kerberos.authhdfs.testcode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;

/**
 * 操作Kerberos认证的HDFS文件系统
 */
public class OperateAuthHDFS {

    public static FileSystem fs = null;

    public static void main(String[] args) throws IOException, InterruptedException {
        final Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://mycluster");

        //设置Kerberos认证
        System.setProperty("java.security.krb5.conf", "D:\\idea_space\\KerberosAuth\\KerberosAuthHDFS\\src\\main\\resources\\krb5.conf");
        UserGroupInformation.loginUserFromKeytab("zhangsan", "D:\\idea_space\\KerberosAuth\\KerberosAuthHDFS\\src\\main\\resources\\zhangsan.keytab");
        UserGroupInformation ugi = UserGroupInformation.getLoginUser();

        //获取HDFS文件系统
        fs = ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
            @Override
            public FileSystem run() throws Exception {
                return FileSystem.get(conf);
            }
        });

        //查看HDFS路径文件
        listHDFSPathDir("/");
        System.out.println("=====================================");

        //创建目录
        mkdirOnHDFS("/kerberos_test");
        System.out.println("=====================================");

        //向HDFS 中写入数据
        writeFileToHDFS("D:\\idea_space\\KerberosAuth\\KerberosAuthHDFS\\data\\test.txt","/kerberos_test/test.txt");
        System.out.println("=====================================");

        //读取HDFS中的数据
        readFileFromHDFS("/kerberos_test/test.txt");
        System.out.println("=====================================");

        //删除HDFS中的目录或者文件
        deleteFileOrDirFromHDFS("/kerberos_test");
        System.out.println("=====================================");

        //关闭fs对象
        fs.close();
    }

    private static void listHDFSPathDir(String hdfsPath) throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(new Path(hdfsPath));
        for (FileStatus fileStatus : fileStatuses) {
            System.out.println(fileStatus.getPath());
        }
    }

    private static void mkdirOnHDFS(String dirpath) throws IOException {
        Path path = new Path(dirpath);

        //判断目录是否存在
        if(fs.exists(path)) {
            System.out.println("目录" + dirpath + "已经存在");
            return;
        }

        //创建HDFS目录
        boolean result = fs.mkdirs(path);
        if(result) {
            System.out.println("创建目录" + dirpath + "成功");
        } else {
            System.out.println("创建目录" + dirpath + "失败");
        }
    }

    private static void writeFileToHDFS(String localFilePath, String hdfsFilePath) throws IOException {
        //判断HDFS文件是否存在，存在则删除
        Path hdfsPath = new Path(hdfsFilePath);
        if(fs.exists(hdfsPath)) {
            fs.delete(hdfsPath, true);
        }

        //创建HDFS文件路径
        Path path = new Path(hdfsFilePath);
        FSDataOutputStream out = fs.create(path);

        //读取本地文件写入HDFS路径中
        FileReader fr = new FileReader(localFilePath);
        BufferedReader br = new BufferedReader(fr);
        String newLine = "";
        while ((newLine = br.readLine()) != null) {
            out.write(newLine.getBytes());
            out.write("\n".getBytes());
        }

        //关闭流对象
        out.close();
        br.close();
        fr.close();
        System.out.println("本地文件 ./data/test.txt 写入了HDFS中的"+path.toString()+"文件中");

    }

    private static void readFileFromHDFS(String hdfsFilePath) throws IOException {
        //读取HDFS文件
        Path path= new Path(hdfsFilePath);
        FSDataInputStream in = fs.open(path);
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        String newLine = "";
        while((newLine = br.readLine()) != null) {
            System.out.println(newLine);
        }

        //关闭流对象
        br.close();
        in.close();
    }

    private static void deleteFileOrDirFromHDFS(String hdfsFileOrDirPath) throws IOException {
        //判断HDFS目录或者文件是否存在
        Path path = new Path(hdfsFileOrDirPath);
        if(!fs.exists(path)) {
            System.out.println("HDFS目录或者文件不存在");
            return;
        }

        //第二个参数表示是否递归删除
        boolean result = fs.delete(path, true);
        if(result){
            System.out.println("HDFS目录或者文件 "+path+" 删除成功");
        } else {
            System.out.println("HDFS目录或者文件 "+path+" 删除成功");
        }

    }

}
