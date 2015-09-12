package com.fission.Spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

public class WordCount {
	private static final FlatMapFunction<String, String> WORDS_EXTRACTOR = new FlatMapFunction<String, String>() {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public Iterable<String> call(String s) throws Exception {
			return Arrays.asList(s.split(" "));
		}
	};

	private static final PairFunction<String, String, Integer> WORDS_MAPPER = new PairFunction<String, String, Integer>() {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public Tuple2<String, Integer> call(String s) throws Exception {
			return new Tuple2<String, Integer>(s, 1);
		}
	};

	private static final Function2<Integer, Integer, Integer> WORDS_REDUCER = new Function2<Integer, Integer, Integer>() {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public Integer call(Integer a, Integer b) throws Exception {
			return a + b;
		}
	};

	public static void main(String[] args) {
		/*
		 * if (args.length < 1) { System.err
		 * .println("Please provide the input file full path as argument");
		 * System.exit(0); }
		 */
		SparkConf conf = new SparkConf().setAppName(
				"com.fission.Spark.WordCount").setMaster("local");
		JavaSparkContext context = new JavaSparkContext(conf);

		JavaRDD<String> file = context
				.textFile("/home/sampath/Documents/workspace-sts-3.6.4.RELEASE/Spark-Examples/wordtest.txt");
		JavaRDD<String> words = file.flatMap(WORDS_EXTRACTOR);
		JavaPairRDD<String, Integer> pairs = words.mapToPair(WORDS_MAPPER);
		JavaPairRDD<String, Integer> counter = pairs.reduceByKey(WORDS_REDUCER);

		counter.saveAsTextFile("/home/sampath/Documents/workspace-sts-3.6.4.RELEASE/Spark-Examples/results");
		context.close();
	}
}