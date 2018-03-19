/*package com.embraces.spark.sql;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

*//**
 * @author tdz
 *
 *//*
public class NetWorkWordCountSQLRunner {

	static final Pattern ptn = Pattern.compile("[ \\.,]+");
	
	public static void main(String[] args) throws InterruptedException {
		SparkConf conf = new SparkConf()
				.setAppName("streaming-sql")
				.setMaster("local[4]");
		JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(5000));
		JavaReceiverInputDStream<String> files = ssc.socketTextStream(
				"192.168.88.131", 9999, StorageLevels.MEMORY_AND_DISK_SER);
		JavaDStream<String> ds = files.flatMap(new FlatMapFunction<String, String>() {
		      @Override
		      public Iterator<String> call(String x) {
		        return Arrays.asList(ptn.split(x)).iterator();
		      }
		    });
		
		ds.foreachRDD(new VoidFunction2<JavaRDD<String>, Time>() {
			@Override
			public void call(JavaRDD<String> rdd, Time time) throws Exception {
				SparkSession session = JavaSparkSessionSingleton.getInstance(rdd.context().getConf());
				JavaRDD<Word> javaRdd = rdd.map(new Function<String, Word>() {
					@Override
					public Word call(String v1) throws Exception {
						String[] arr = v1.split(":");
						Word word = new Word();
						word.setName(null);
//						word.setAge(Integer.parseInt(arr[1]));
						return word;
					}
				});
				
				
				Dataset<Row> ds = session.createDataFrame(javaRdd, Word.class);
				ds.createOrReplaceTempView("words");
				
				Dataset<Row> retDS = session.sql("select name, count(1) total from words group by name order by name");
				retDS.printSchema();
				System.out.println("----------------" + time + "----------------");
				retDS.show();
			}
		});
		
		ssc.start();
		ssc.awaitTermination(); 
	}

	public static class Word implements Serializable {

		private static final long serialVersionUID = 1L;
		private String name = "";
		private int age;
		
		public int getAge() {
			return age;
		}

		public void setAge(int age) {
			this.age = age;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public Word() {
			super();
		}

		public Word(String name) {
			super();
			this.name = name;
		}
	}
	
	*//** Lazily instantiated singleton instance of SparkSession *//*
	public static class JavaSparkSessionSingleton {
	  private static transient SparkSession instance = null;
	  public static SparkSession getInstance(SparkConf sparkConf) {
	    if (instance == null) {
	      instance = SparkSession
	        .builder()
	        .config(sparkConf)
	        .getOrCreate();
	    }
	    return instance;
	  }
	}
}
*/