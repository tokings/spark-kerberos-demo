package com.embraces.spark.streaming;

import java.util.Arrays;
import java.util.Iterator;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.embraces.spark.KerberosAuthenticator;

import scala.Tuple2;

/**
 * SparkStreaming应用示例代码
 * @author	tokings.tang@embracesource.com
 * @date	2018年3月19日 上午11:42:20
 * @copyright	http://www.embracesource.com
 */
public class SparkStreamApp {

	// 行记录切割字符
	private static final Pattern SPACE = Pattern.compile("[ \\.]");

	public static void main(String[] args) throws Exception {

		if (args.length < 2) {
			System.err.println("Usage: SparkStreamApp <hostname> <port>");
			System.exit(1);
		}

		// 认证类型
		String authenticationType = "kerberos";
		// 认证服务器连接信息文件路径
		String krb5FilePath = "krb5.ini";
		// 认证主体
		String principal = "hive/master.embracesource.com@EXAMPLE.COM";
		// 认证密钥文件
		String keytab = "hive.service.keytab";
		
		// 进行Kerberos认证
		KerberosAuthenticator.authentication(authenticationType, krb5FilePath, principal, keytab);

		// 初始化Spark配置
		SparkConf sparkConf = new SparkConf();
		
		// 设置应用名和集群模式
		sparkConf.setAppName("SparkStreamApp").setMaster("local[*]");

		JavaStreamingContext ssc = null;
		
		try {
			// 初始化Stream上下文，时间间隔5秒
			ssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));

			// 创建输入流，读取host和port的数据，设置数据存储模式
			JavaReceiverInputDStream<String> lines = ssc.socketTextStream(args[0], Integer.parseInt(args[1]),
					StorageLevels.MEMORY_AND_DISK_SER);

			// 调用函数进行切割数据
			JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
				private static final long serialVersionUID = 1L;

				@Override
				public Iterator<String> call(String x) {
					return Arrays.asList(SPACE.split(x)).iterator();
				}
			});
			
			// 统计切割后的数据出现次数
			JavaPairDStream<String, Integer> wordCounts = words.mapToPair(new PairFunction<String, String, Integer>() {
				private static final long serialVersionUID = 1L;

				@Override
				public Tuple2<String, Integer> call(String s) {
					return new Tuple2<>(s, 1);
				}
			}).reduceByKey(new Function2<Integer, Integer, Integer>() {
				private static final long serialVersionUID = 1L;

				@Override
				public Integer call(Integer i1, Integer i2) {
					return i1 + i2;
				}
			});

			// 打印统计结果
			wordCounts.print();
			// 开始读取数据和计算
			ssc.start();
			// 等待中断操作
			ssc.awaitTermination();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			// 关闭Spark流上下文
			if(ssc != null) {
				ssc.close();
			}
		}
	}
}
