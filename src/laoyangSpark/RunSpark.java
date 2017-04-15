package laoyangSpark;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class RunSpark {
	
	public static void main(String[] args) throws IllegalArgumentException, IOException {
		//<input> <output> <train_percent> <ranks> <lambda> <iteration>
		String[] inputArgs= new String[]{
				"hdfs://node1:8020/user/root/ratings.dat",
				"hdfs://node1:8020/user/fansy/als_output",
				"0.8",
				"10",
				"10.0",
				"20"
		};
		String[] runArgs=new String[]{
                "--name","ALS Model Train ",
                "--class","als.ALSModelTrainer",
                //@TODO �˲����ڲ���ʱʹ�ã�����Ӧע��
                "--driver-memory","512m",
                "--num-executors", "2",
                "--executor-memory", "512m",
                "--jar","hdfs://node1:8020/user/root/Spark141-als.jar",//
                //// Spark ���ӽڵ�����driverʱ��ֻ��ȡspark-assembly-1.4.1-hadoop2.6.0.jar�е������ļ���
                "--files","hdfs://node1:8020/user/root/yarn-site.xml",
                "--arg",inputArgs[0],
                "--arg",inputArgs[1],
                "--arg",inputArgs[2],
                "--arg",inputArgs[3],
                "--arg",inputArgs[4],
                "--arg",inputArgs[5]
        };
		FileSystem.get(Utils.getConf()).delete(new Path(inputArgs[1]), true);
		Utils.runSpark(runArgs);
	}
	
	
}