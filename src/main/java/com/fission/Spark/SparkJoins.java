package com.fission.Spark;

//http://stackoverflow.com/questions/28338694/apache-spark-joins-example-with-java

import java.io.FileNotFoundException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.google.common.base.Optional;

public class SparkJoins {

	@SuppressWarnings("serial")
	public static void main(String[] args) throws FileNotFoundException {
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName(
				"Spark Count").setMaster("local"));
		JavaRDD<String> customerInputFile = sc.textFile("customers_data.txt");
		JavaPairRDD<String, String> customerPairs = customerInputFile
				.mapToPair(new PairFunction<String, String, String>() {
					public Tuple2<String, String> call(String s) {
						String[] customerSplit = s.split(",");
						return new Tuple2<String, String>(customerSplit[0],
								customerSplit[1]);
					}
				}).distinct();

		JavaRDD<String> transactionInputFile = sc
				.textFile("transactions_data.txt");
		JavaPairRDD<String, String> transactionPairs = transactionInputFile
				.mapToPair(new PairFunction<String, String, String>() {
					public Tuple2<String, String> call(String s) {
						String[] transactionSplit = s.split(",");
						return new Tuple2<String, String>(transactionSplit[2],
								transactionSplit[3] + "," + transactionSplit[1]);
					}
				});

		// JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> rddWithJoin =
		// .leftOuterJoin(secondRDD);

		// Default Join operation (Inner join)
		JavaPairRDD<String, Tuple2<String, String>> joinsOutput = customerPairs
				.join(transactionPairs);
		System.out.println("Joins function Output: " + joinsOutput.collect());

		// Left Outer join operation
		JavaPairRDD<String, Iterable<Tuple2<String, Optional<String>>>> leftJoinOutput = customerPairs
				.leftOuterJoin(transactionPairs).groupByKey().sortByKey();
		System.out.println("LeftOuterJoins function Output: "
				+ leftJoinOutput.collect());

		// Right Outer join operation
		JavaPairRDD<String, Iterable<Tuple2<Optional<String>, String>>> rightJoinOutput = customerPairs
				.rightOuterJoin(transactionPairs).groupByKey().sortByKey();
		System.out.println("LeftOuterJoins function Output: "
				+ rightJoinOutput.collect());

		sc.close();
	}
}