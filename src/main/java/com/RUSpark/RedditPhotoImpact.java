package com.RUSpark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

/* any necessary Java packages here */

public class RedditPhotoImpact {

	public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: RedditPhotoImpact <file>");
      System.exit(1);
    }
		
		String InputPath = args[0];
		
		/* Implement Here */ 
		final String DELIMITER = ",";
	    SparkSession spark = SparkSession
	      .builder()
	      .appName("RedditPhotoImpact")
	      .getOrCreate();

	    JavaRDD<String> lines = spark.read().textFile(InputPath).javaRDD();
	    
	    JavaPairRDD<Integer, Integer> stats = lines.mapToPair(line -> {
	    	String[] _line = line.split(DELIMITER);
	    	return new Tuple2<>(Integer.parseInt(_line[0]), 
	    			Integer.parseInt(_line[_line.length - 3]) +
    				Integer.parseInt(_line[_line.length - 2]) + 
    				Integer.parseInt(_line[_line.length - 1]));
	    });
	   
	    JavaPairRDD<Integer, Integer> counts = stats.reduceByKey((value1, value2) -> value1 + value2);
	    // List<Tuple2<Integer, Integer>> results = counts.sortByKey().collect();
	    List<String> manual = lines.collect();
	    spark.stop();
	    /*
	    for (Tuple2<Integer, Integer> result : results) {
	    	System.out.println(result._1() + " " + result._2());
	    }
	    */
	    
	    HashMap<Integer, Integer> impacts = new HashMap<>();
	    for (String _line : manual) { 
	    	String[] line = _line.split(DELIMITER);
	    	Integer key = Integer.parseInt(line[0]);
	    	if (!impacts.containsKey(key)) {
	    		impacts.put(key, 0);
	    	}
	    	impacts.put(key, impacts.get(key) + Integer.parseInt(line[line.length - 3]) +
    				Integer.parseInt(line[line.length - 2]) + 
    				Integer.parseInt(line[line.length - 1]));
	    }
 	   System.out.println("Correct Impacts");
 	   Integer[] keys = (Integer[]) impacts.keySet().toArray(new Integer[impacts.size()]);
  	   Arrays.sort(keys);
  	   for (Integer key : keys) {
  		   System.out.println(key + " " + impacts.get(key));
  	   }
  	   
	}

}
