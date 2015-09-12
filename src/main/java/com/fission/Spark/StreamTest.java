package com.fission.Spark;

import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

import com.google.common.collect.Lists;

public class StreamTest {

	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String args[]) {

		SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount")
				.setMaster("local[2]");
		// Create the context with a 1 second batch size
		JavaStreamingContext streamingContext = new JavaStreamingContext(
				sparkConf, new Duration(20000));

		// We are basically reading from the command line as stream for this run
		// (nc -l localhost 50050) in command line
		JavaReceiverInputDStream<String> messages = streamingContext
				.socketTextStream("localhost", 50050);

		JavaDStream<String> words = messages
				.flatMap(new FlatMapFunction<String, String>() {
					private static final long serialVersionUID = 1L;

					public Iterable<String> call(String x) {
						return Lists.newArrayList(SPACE.split(x));
					}
				});

		JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
				new PairFunction<String, String, Integer>() {
					private static final long serialVersionUID = 1L;

					public Tuple2<String, Integer> call(String s) {
						return new Tuple2<String, Integer>(s, 1);
					}
				}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});

		wordCounts.print();
		streamingContext.start();
		streamingContext.awaitTermination();
		streamingContext.close();
	}
}
