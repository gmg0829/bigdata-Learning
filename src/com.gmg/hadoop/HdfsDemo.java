package com.gmg.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.File;
import java.io.IOException;

/**
 * @author gmg
 * @title: HdfsDEmo
 * @projectName bigdata-Learning
 * @description: TODO
 * @date 2019/12/24 15:52
 */
public class HdfsDemo {

    private  static Configuration conf=null;

    public static String PATH_TO_HDFS_SITE_XML;

    public static String PATH_TO_CORE_SITE_XML;

    static {
        String path = System.getProperty("java.class.path");

        int firstIndex = path.lastIndexOf(System.getProperty("path.separator")) + 1;
        int lastIndex = path.lastIndexOf(File.separator) + 1;
        path = path.substring(firstIndex, lastIndex);
        PATH_TO_HDFS_SITE_XML = path + "core-site.xml";
        PATH_TO_CORE_SITE_XML = path + "hdfs-site.xml";

        conf=new Configuration();
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.addResource(new Path(PATH_TO_HDFS_SITE_XML));
        conf.addResource(new Path(PATH_TO_CORE_SITE_XML));
    }

    public static void put(String remotePath, String localPath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path src = new Path(localPath);
        Path dst = new Path(remotePath);

        fs.copyFromLocalFile(src, dst);
        fs.close();
    }

    public static void cat(String remotePath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(remotePath);

        if (fs.exists(path)) {
            FSDataInputStream is = fs.open(path);
            FileStatus fileStatus = fs.getFileStatus(path);

            byte[] buffer = new byte[Integer.parseInt(String.valueOf(fileStatus.getLen()))];

            is.readFully(0, buffer);

            fs.close();

            System.out.println(buffer.toString());
        }
    }

    public static void get(String remotePath, String localPath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path src = new Path(localPath);
        Path dst = new Path(remotePath);

        fs.copyToLocalFile(src, dst);
        fs.close();
    }

    public static void rm(String remotePath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(remotePath);

        fs.delete(path, true);
        fs.close();
    }

    public static void ls(String remotePath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(remotePath);

        FileStatus[] fileStatus = fs.listStatus(path);

        Path[] listPaths=FileUtil.stat2Paths(fileStatus);

        for (Path p:listPaths){
            System.out.println(p);
        }

        fs.close();
    }

    public static void mkdir(String remotePath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(remotePath);

        fs.create(path);

        fs.close();
    }



}
