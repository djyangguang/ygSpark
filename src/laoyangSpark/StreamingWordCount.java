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
		 //һ��StreamingContext���󱻴��������й��ܵ���Ҫ��ڵ�
		// Create a local StreamingContext with two working thread and batch interval of 1 second
		// SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
	SparkConf  conf = new SparkConf().setMaster("spark://192.168.62.128:7077").setAppName("laoyangapp");
	JavaStreamingContext jssc = new JavaStreamingContext(conf,new Duration(1000));
	//���ǿ��Դ���һ��dstream��ʾ��һ��TCP��������ָ��������
	JavaReceiverInputDStream<String> line = jssc.socketTextStream("192.168.62.128", 9999);
	//����dstream��ʾ�����ݽ��ӷ��������յ����������������е�ÿ����¼����һ���ı���Ȼ��������ѿռ�ָ������
	JavaDStream<String> words = line.flatMap(
			  new FlatMapFunction<String, String>() {
			    @Override public Iterator<String> call(String x) {
			      return Arrays.asList(x.split(".")).iterator();
			    }
			  });
//	flatmap��dstream���������ɶ���¼�¼ÿ����¼Դ�д���һ���µ�dstream dstream��
//	����������£�ÿһ�ж������ָ�ɶ���ʺʹʵ���Ϊ����Ļ�dstream��
//	��ע�⣬���Ƕ���ı任ʹ��flatmapfunction�������ǻᷢ��ǰ���ĵ�·�ϣ�
//	����������ı������java API����������dstream�任
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
