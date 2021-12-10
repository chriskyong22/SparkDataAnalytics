package com.RUSpark;

import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

/* any necessary Java packages here */

public class NetflixMovieAverage {

	public static void main(String[] args) throws Exception {

	    if (args.length < 1) {
	      System.err.println("Usage: NetflixMovieAverage <file>");
	      System.exit(1);
	    }
		
		String InputPath = args[0];
		
		/* Implement Here */ 
		final String DELIMITER = ",";
		SparkSession spark = SparkSession
		      .builder()
		      .appName("NetflixMovieAverage")
		      .getOrCreate();
		
		JavaRDD<String> lines = spark.read().textFile(InputPath).javaRDD();
		
		JavaPairRDD<Integer, Integer> ratings = lines.mapToPair(line -> new Tuple2<>(Integer.parseInt(line.split(DELIMITER)[0]), Integer.parseInt(line.split(DELIMITER)[2])));
		
		JavaPairRDD<Integer, List<Integer>> ratingsPerMovie = ratings.aggregateByKey(new ArrayList<Integer>(), 
				(list, value) -> {
					list.add(value);
					return list;
				}, (list1, list2) -> {
					list1.addAll(list2);
					return list1;
				});
		// JavaPairRDD<Integer, Iterable<Integer>> ratingsPerMovie = ratings.groupByKey();
		
		JavaPairRDD<Integer, Double> averages = ratingsPerMovie.mapToPair(rating -> {
			double sum = 0;
			for (Integer value : rating._2) {
				sum += value;
			}
			return new Tuple2<Integer, Double> (rating._1, sum / rating._2.size());
		});
		
		List<Tuple2<Integer, Double>> values = averages.sortByKey().collect();
		// List<String> manual = lines.collect();
		spark.stop();
		
		DecimalFormat decimalFormatFloor = new DecimalFormat("#.##");
		decimalFormatFloor.setRoundingMode(RoundingMode.FLOOR);
		
		for (Tuple2<Integer, Double> value : values) {
			System.out.println(value._1() + " " + decimalFormatFloor.format(value._2()));
		}
		
		/*
		HashMap<Integer, Tuple2<Double, Integer>> ratingsM = new HashMap<>();
		for (String line : manual) {
			Integer key = Integer.valueOf(line.split(DELIMITER)[0]);
			if (!ratingsM.containsKey(key)) {
				ratingsM.put(key, new Tuple2<Double, Integer> (0.0, 0));
			}
			ratingsM.put(key, new Tuple2<Double, Integer> (ratingsM.get(key)._1 + Integer.valueOf(line.split(DELIMITER)[2]), ratingsM.get(key)._2 + 1));
		}
		
		HashMap<Integer, Double> averagesM = new HashMap<>();
		for (Integer key : ratingsM.keySet()) {
			double average = ratingsM.get(key)._1() / ratingsM.get(key)._2();
			averagesM.put(key, average);
		}
		
  	   Integer[] keys = (Integer[]) averagesM.keySet().toArray(new Integer[averagesM.size()]);
  	   Arrays.sort(keys);
  	   for (Integer key : keys) {
  		   System.out.println(key + " " + decimalFormatFloor.format(averagesM.get(key)));
  	   }
		 */
	}

}
