package com.gtja.hadoop;

import java.io.FileInputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HelloHDFS {

    public static Log log =  LogFactory.getLog(HelloHDFS.class);

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.56.100:9000");
        conf.set("dfs.replication", "2");//默认为3
        FileSystem fileSystem = FileSystem.get(conf);

        boolean success = fileSystem.mkdirs(new Path("/yucong"));
        log.info("创建文件是否成功:" + success);

        success = fileSystem.exists(new Path("/yucong"));
        log.info("文件是否存在:" + success);

        success = fileSystem.delete(new Path("/yucong"), true);
        log.info("删除文件是否成功：" + success);

        /*FSDataOutputStream out = fileSystem.create(new Path("/test.data"), true);
        FileInputStream fis = new FileInputStream("c:/test.txt");
        IOUtils.copyBytes(fis, out, 4096, true);*/

        FSDataOutputStream out = fileSystem.create(new Path("/test2.data"));
        FileInputStream in = new FileInputStream("F:\\Hadoop\\代码\\test.txt");
        byte[] buf = new byte[4096];
        int len = in.read(buf);
        while(len != -1) {
            out.write(buf,0,len);
            len = in.read(buf);
        }
        in.close();
        out.close();

        FileStatus[] statuses = fileSystem.listStatus(new Path("/"));
        log.info(statuses.length);
        for(FileStatus status : statuses) {
            log.info(status.getPath());
            log.info(status.getPermission());
            log.info(status.getReplication());
        }
    }

}
