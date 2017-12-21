package com.gtja.mahout;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;

public class Recommend {

    public static final String HDFS = "hdfs://192.168.56.100:9000";
    public static final Pattern DELIMITER = Pattern.compile("[\t,]");

    public static void main(String[] args) throws Exception {
        Map<String, String> path = new HashMap<String, String>();
        path.put("data", "E:/test/mahout/u1.test");
        path.put("Step1Input", HDFS + "/user/hdfs/recommend");
        path.put("Step1Output", path.get("Step1Input") + "/step1");
        path.put("Step2Input", path.get("Step1Output"));
        path.put("Step2Output", path.get("Step1Input") + "/step2");
        path.put("Step3Input1", path.get("Step1Output"));
        path.put("Step3Output1", path.get("Step1Input") + "/step3_1");
        path.put("Step3Input2", path.get("Step2Output"));
        path.put("Step3Output2", path.get("Step1Input") + "/step3_2");
        path.put("Step4Input1", path.get("Step3Output1"));
        path.put("Step4Input2", path.get("Step3Output2"));
        path.put("Step4Output", path.get("Step1Input") + "/step4");

        Step1.run(path);
        Step2.run(path);
        Step3.run1(path);
        Step3.run2(path);
        Step4.run(path);
        System.exit(0);
    }

    public static JobConf config() {

        JobConf conf = new JobConf();
        //执行远端文件
        conf.set("fs.defaultFS", "hdfs://192.168.56.100:9000");
        //conf.set("dfs.replication", "2");//默认为3
        //远端执行
        conf.set("mapreduce.job.jar", "target/hadoop.jar");
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("yarn.resourcemanager.hostname", "master");
        conf.set("mapreduce.app-submission.cross-platform", "true");
        /*JobConf conf = new JobConf(Recommend.class);
        conf.setJobName("Recommend");
        conf.addResource("classpath:/hadoop/core-site.xml");
        conf.addResource("classpath:/hadoop/core-site.xml.xml");
        conf.addResource("classpath:/hadoop/mapred-site.xml");*/
        return conf;
    }

}
