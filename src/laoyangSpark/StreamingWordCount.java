package laoyangSpark;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import breeze.macros.expand.args;
import scala.Tuple2;


 class StreamingWordCount {
	
	 public static void main(String[] args) throws Exception {
		 //一个StreamingContext对象被创建，所有功能的主要入口点
		// Create a local StreamingContext with two working thread and batch interval of 1 second
		// SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
	SparkConf  conf = new SparkConf().setMaster("spark://192.168.62.128:7077").setAppName("laoyangapp");
	JavaStreamingContext jssc = new JavaStreamingContext(conf,new Duration(1000));
	//我们可以创建一个dstream表示从一个TCP数据流，指定主机名
	JavaReceiverInputDStream<String> line = jssc.socketTextStream("192.168.62.128", 9999);
	//这条dstream表示的数据将从服务器接收到的数据流。此流中的每个记录都是一行文本。然后，我们想把空间分割成文字
	JavaDStream<String> words = line.flatMap(
			  new FlatMapFunction<String, String>() {
			    @Override public Iterator<String> call(String x) {
			      return Arrays.asList(x.split(".")).iterator();
			    }
			  });
//	flatmap是dstream操作，生成多个新记录每个记录源中创建一个新的dstream dstream。
//	在这种情况下，每一行都将被分割成多个词和词的流为代表的话dstream。
//	请注意，我们定义的变换使用flatmapfunction对象。我们会发现前进的道路上，
//	有许多这样的便利类的java API，帮助定义dstream变换
	JavaPairDStream<String ,Integer> pairs = words.mapToPair(
			new PairFunction<String,String,Integer>(){

				@Override
				public Tuple2<String, Integer> call(String s) throws Exception {
					// TODO Auto-generated method stub
					return new Tuple2<>(s,1);
				}
				
				
			}
			
			);
	JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(
			  new Function2<Integer, Integer, Integer>() {
			    @Override public Integer call(Integer i1, Integer i2) {
			      return i1 + i2;
			    }
			  });
	
	
		
	wordCounts.print();
	jssc.start();// Start the computation
	jssc.awaitTermination();// Wait for the computation to terminate
	 }
}
