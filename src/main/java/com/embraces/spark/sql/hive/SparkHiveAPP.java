package com.embraces.spark.sql.hive;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import com.embraces.spark.KerberosAuthenticator;

/**
 * SparkSQL操作Hive测试应用
 * @author	tokings.tang@embracesource.com
 * @date	2018年3月19日 上午10:35:53
 * @copyright	http://www.embracesource.com
 */
public class SparkHiveAPP {

	public static void main(String[] args) {

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
		SparkConf conf = new SparkConf();
		
		// 指定spark yarn集群使用的依赖包路径，防止公用依赖重复提交
		conf.set("spark.yarn.jars", "hdfs://master.embracesource.com:8020/system/sparkJar/jars/*.jar");
		
		// 创建SparkSql会话
		SparkSession sqlSesssion = SparkSession
				.builder()
				// 指定集群模式
				.master("local[2]")
//				.master("yarn")
				// 指定应用名称
				.appName("spark-hive")
				// 设置其他Spark配置信息
				.config(conf)
				// 是sparksql支持Hive操作
				.enableHiveSupport()
				.getOrCreate();
		
		// 执行SQL语句
		sqlSesssion.sql("SELECT * FROM test limit 10").show();
		
		// 停止会话
		sqlSesssion.stop();
	}
}
