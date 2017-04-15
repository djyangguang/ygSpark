package laoyangSpark;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class yarncount {
	public static final String HDFS_PATH = "hdfs://192.168.62.128:9000/laoyang";
	public static final String FILE_PATH = "/laoyang03/";
	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) throws Exception {
		System.setProperty("HADOOP_USER_NAME","root");//重要
		   System.setProperty("SPARK_YARN_MODE", "true");
		if (args.length < 1) {
			System.err.println("Usage: JavaWordCount <file>");
			System.exit(1);
		}
		uploadFile();
		//FileSystem fileSystem = FileSystem.get(new URI(HDFS_PATH), new Configuration());

		//FSDataOutputStream out = fileSystem.create(new Path(FILE_PATH));
		//System.out.println(new Path(FILE_PATH));
		//final FileInputStream in = new FileInputStream("D:\\spark-package\\yarncount.jar");
		//IOUtils.copyBytes(in, out, 1024, true);


		SparkConf sparkConf = new SparkConf().setAppName("yarnjavacount01");
		sparkConf.setMaster("yarn");
		sparkConf.set("spark.yarn.dist.files", "D:\\Spark_workspace\\laoyangSpark\\src\\yarn-site.xml");
		sparkConf.set("spark.yarn.jars", "hdfs://192.168.62.128:9000//laoyang/Sparkjars//*");//要正确 /home/hadoop/hadoop-yarn/containers/application_1488437341345_0013/container_1488437341345_0013_02_000001
		//sparkConf.set("spark.executor.memory", "512m");
		
		//sparkConf.set("spark.executor.cores", "1");
		
		// sparkConf.set("spark.yarn.scheduler.heartbeat.interval-ms","1000");
		// 错误：Exception in thread "main" org.apache.spark.SparkException:Only one SparkContext may be running in this JVM (see SPARK-2243). To ignore this error, set spark.driver.allowMultipleContexts = true. The currently 
		sparkConf.set("spark.driver.allowMultipleContexts","true");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		ctx.addJar("D:\\spark-package\\yarncount.jar");
		JavaRDD<String> lines = ctx.textFile(args[0], 1);
		//JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterator<String> call(String s) {
				return Arrays.asList(SPACE.split(s)).iterator();
			}
		});

		JavaPairRDD<String, Integer> ones = words.mapToPair(
				new PairFunction<String, String, Integer>() {
					@Override
					public Tuple2<String, Integer> call(String s) {
						return new Tuple2<>(s, 1);
					}
				});

		JavaPairRDD<String, Integer> counts = ones.reduceByKey(
				new Function2<Integer, Integer, Integer>() {
					@Override
					public Integer call(Integer i1, Integer i2) {
						return i1 + i2;
					}
				});

		List<Tuple2<String, Integer>> output = counts.collect();
		for (Tuple2<?,?> tuple : output) {
			System.out.println("==============yarn结果==============="+tuple._1() + ": " + tuple._2());
		}
		ctx.stop();
	}
	public static void uploadFile()throws Exception{//加载默认配置
		  Configuration conf=new Configuration();
		FileSystem fs=FileSystem.get(conf);//本地文件
		//Path src =new Path("D:\\spark-package\\yarncount.jar");
		//Path src = new Path("C:\\Users\\dj\\Desktop\\yarn\\");
		Path src = new Path("D:\\aaa\\");//会把所有文件下的都传上去 D:\Sparkjars\examples\src\main\resources
		
		//Path src = new Path("D:\\BaiduYunDownload\\saledata\\");
		//HDFS为止
		Path dst =new Path("hdfs://192.168.62.128:9000/laoyang"); //放到那个位置
		try {
			fs.copyFromLocalFile(src, dst);
			} catch (IOException e) {// TODO Auto-generated catch blocke.printStackTrace();}
		}
		System.out.println("上传成功........");
		fs.close();//释放资源
		}
}
