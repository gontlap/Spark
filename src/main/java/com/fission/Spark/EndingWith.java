package com.fission.Spark;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class EndingWith {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("WordCount").setMaster(
				"local");
		JavaSparkContext context = new JavaSparkContext(conf);

		JavaRDD<String> listRdd = context.parallelize(Arrays.asList("chanti",
				"ganesh", "naresh", "ritesh", "fission_labs", "value_labs",
				"pega systems"));
		System.out.println("ListRDD before calling an action: " + listRdd);

		JavaRDD<String> endingWithSh = listRdd
				.filter(new Function<String, Boolean>() {

					private static final long serialVersionUID = 1L;

					public Boolean call(String item) throws Exception {
						if (item.endsWith("sh"))
							return true;
						return false;
					}
				});
		System.out.println(endingWithSh.collect());

		JavaRDD<String> endingWithLabs = listRdd
				.filter(new Function<String, Boolean>() {

					private static final long serialVersionUID = 1L;

					public Boolean call(String item) throws Exception {
						if (item.endsWith("labs"))
							return true;
						return false;
					}
				});

		System.out.println(endingWithLabs.collect());
		context.close();
	}
}