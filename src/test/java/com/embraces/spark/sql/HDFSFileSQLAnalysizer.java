package com.embraces.spark.sql;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class HDFSFileSQLAnalysizer {
	
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("hdfs-test").setMaster("local[4]");
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
		
		Dataset<String> ds = spark.read().textFile("hdfs://172.16.49.241:8020"
				+ "/tmp/public_hue_examples/2015_11_18/web_logs_1.csv");
		
		JavaRDD<Row> rdd = ds.javaRDD()
				.map(new Function<String, Row>() {
					@Override
					public Row call(String val) throws Exception {
//						System.out.println(val);
						String[] values = val.split(",");
						return RowFactory.create(values);
					}
				});
//		
	/*	JavaFutureAction<List<Row>> listRows = rdd.collectAsync();
		try {
			List<Row> row = listRows.get();
			row.forEach(new Consumer<Row>() {
				@Override
				public void accept(Row t) {
					System.out.println(t.schema());
					for(int i=0;i<t.length();i++) {
						System.out.print(t.get(i) + ":" + t.get(i).getClass() + "\t");
					}
					System.out.println();
				}
			});
		} catch (Exception e) {
			e.printStackTrace();
		}*/
		
		String schemaStr = "c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,"
				+ "c11,c12,c13,c14,c15,c16,c17,c18,c19,c20,"
				+ "c21,c22,c23,c24,c25,c26,c27,c28,";
		List<StructField> fields = new ArrayList<StructField>();
		for(String schemaField : schemaStr.split(",")) {
			fields.add(DataTypes.createStructField(schemaField, DataTypes.StringType, true));
		}
		StructType schema = DataTypes.createStructType(fields);
		Dataset<Row> dataset = spark.createDataFrame(rdd, schema);
//		dataset.show();
		dataset.createOrReplaceTempView("web_logs");
		
//		spark.sql("select c15, count(1) total from web_logs group by c15 order by total").show();
		Dataset<Row> ret = spark.sql("select c6 , c21, count(1) total, sum(c21) sums from web_logs group by c6, c21");
		ret.printSchema();
		ret.show();
		
		try {
			List<Row> rows = ret.javaRDD().collectAsync().get();
			rows.forEach(new Consumer<Row>() {
				@Override
				public void accept(Row t) {
					System.out.println(t.schema());

					for(int i=0; i<t.length(); i++) {
						System.out.print(t.get(i) + ":" + t.getAs(i) + "\t");
					}
					System.out.println();
				}
			});
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		/*ds.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterator<String> call(String t) throws Exception {
				List<String> ret = new ArrayList<String>();
				String[] fields = t.split("[,]");
				for(String field : fields) {
					ret.add(field);
				}
				return ret.iterator();
			}
		}, Encoders.STRING())
		.show();*/
		
	}
}
