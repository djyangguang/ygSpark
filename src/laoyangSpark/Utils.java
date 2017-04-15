package laoyangSpark;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.yarn.Client;
import org.apache.spark.deploy.yarn.ClientArguments;

public class Utils {
	private static  Configuration configuration = null;

	public static Configuration getConf(){
		if(configuration==null){
			
			configuration = new Configuration();
			configuration.setBoolean("mapreduce.app-submission.cross-platform", true);// 配置使用跨平台提交任务
			configuration.set("fs.defaultFS", "hdfs://node1:8020");// 指定namenode
			configuration.set("mapreduce.framework.name", "yarn"); // 指定使用yarn框架
			configuration.set("yarn.resourcemanager.address", "node1:8032"); // 指定resourcemanager
			configuration.set("yarn.resourcemanager.scheduler.address", "node1:8030");// 指定资源分配器
			configuration.set("mapreduce.jobhistory.address", "node2:10020");// 指定historyserver
		}
		
		return configuration;
	}
	
	/**
	 * 调用Spark
	 * @param args
	 * @return
	 */
	public static boolean runSpark(String[] args){
        try {
            System.setProperty("SPARK_YARN_MODE", "true");
            SparkConf sparkConf = new SparkConf();
            sparkConf.set("spark.yarn.jar",
            		"hdfs://node1:8020/user/root/spark-assembly-1.4.1-hadoop2.6.0.jar");
            sparkConf.set("spark.yarn.scheduler.heartbeat.interval-ms",
            		"1000");
            
            ClientArguments cArgs = new ClientArguments(args);
            ClientArguments cAtgs = new ClientArguments(args);
            new Client(cArgs, getConf(), sparkConf).run();
        }catch(Exception e){
            e.printStackTrace();
            return false;
        }
        return true ;
    }
}
