package com.RUSpark;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

/* any necessary Java packages here */

public class RedditHourImpact {

	public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: RedditHourImpact <file>");
      System.exit(1);
    }
		
		String InputPath = args[0];
		
		/* Implement Here */ 
		final String DELIMITER = ",";
	    SparkSession spark = SparkSession
	  	      .builder()
	  	      .appName("RedditHourImpact")
	  	      .getOrCreate();
	    
  	    JavaRDD<String> lines = spark.read().textFile(InputPath).javaRDD();
  	    /*
  	    JavaPairRDD<Integer, Integer> data = lines.mapToPair(line -> {
  	    	String[] _line = line.split(DELIMITER);
  	    	return new Tuple2<Integer, Integer>(Instant.ofEpochSecond(Long.parseLong(_line[1]))
						.atZone(ZoneId.of("America/New_York"))
						.toLocalTime()
						.getHour(), 
					Integer.parseInt(_line[_line.length - 3]) + 
  					Integer.parseInt(_line[_line.length - 2]) + 
  					Integer.parseInt(_line[_line.length - 1]));
  	    });
  	    
  	    JavaRDD<Long> hoursRange = spark.range(0, 24).javaRDD();
  	    
  	    JavaPairRDD<Integer, Integer> hours = hoursRange.mapToPair(hour -> new Tuple2<Integer, Integer>(hour.intValue(), 0));
  	    
  	    JavaPairRDD<Integer, Integer> merged = hours.union(data);
  	    
  	    JavaPairRDD<Integer, Integer> stats = merged.reduceByKey((value1, value2) -> value1 + value2);
  	    List<Tuple2<Integer, Integer>> times = stats.sortByKey().collect();
  	    */
  	    List<String> manual = lines.collect();
  	    spark.stop();
  	    /*
  	    for (Tuple2<Integer, Integer> time : times) {
  	    	System.out.println(time._1() + " " + time._2());
  	    }
  	    */
  	    
  	    HashMap<Integer, Integer> timesM = new HashMap<>();
	   	for (int hour = 0; hour <= 23; hour++) {
	   		timesM.put(hour, 0);
	  	}
	   	HashMap<Long, List<Integer>> hoursM = new HashMap<>();
  	    for (String line : manual) {
  	    	int key = Instant.ofEpochSecond(Long.parseLong(line.split(DELIMITER)[1]))
  					.atZone(ZoneId.of("America/New_York"))
  					.toLocalTime()
  					.getHour();
  	    	if (!hoursM.containsKey(Long.parseLong(line.split(DELIMITER)[1]))) {
  	    		hoursM.put(Long.parseLong(line.split(DELIMITER)[1]), new ArrayList<Integer>());
  	    	}
  	    	hoursM.get(Long.parseLong(line.split(DELIMITER)[1])).add(key);
  	    	timesM.put(key, timesM.get(key) + Integer.parseInt(line.split(DELIMITER)[line.split(DELIMITER).length - 3]) + 
  					Integer.parseInt(line.split(DELIMITER)[line.split(DELIMITER).length - 2]) + 
  					Integer.parseInt(line.split(DELIMITER)[line.split(DELIMITER).length - 1]));
  	   }

  	    
  	   Integer[] keys = (Integer[]) timesM.keySet().toArray(new Integer[timesM.size()]);
  	   Arrays.sort(keys);
  	   for (Integer key : keys) {
  		   System.out.println(key + " " + timesM.get(key));
  	   }
  	   /*
 	   System.out.println("Unix To Time");
 	   Long[] _keys = (Long[]) hoursM.keySet().toArray(new Long[hoursM.size()]);
  	   Arrays.sort(_keys);
  	   for (Long key : _keys) {
  		   for (Integer value : hoursM.get(key)) {
  			   System.out.println(key + " " + value);
  		   }
  	   }
  	   */
	}

}
