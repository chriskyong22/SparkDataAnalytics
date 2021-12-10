package com.RUSpark;

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

public class NetflixGraphGenerate {

	public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: NetflixGraphGenerate <file>");
      System.exit(1);
    }
		
		String InputPath = args[0];
		
		/* Implement Here */ 
		final String DELIMITER = ",";
	    SparkSession spark = SparkSession
	  	      .builder()
	  	      .appName("NetflixGraphGenerate")
	  	      .getOrCreate();

  	    JavaRDD<String> lines = spark.read().textFile(InputPath).javaRDD();
  	    JavaPairRDD<String, Integer> movieRatings = lines.mapToPair(line -> {
  	    	return new Tuple2<> (line.split(DELIMITER)[0] + " " + line.split(DELIMITER)[2], Integer.parseInt(line.split(DELIMITER)[1]));   	
  	    });
  	    
  	    
  	    
  	    JavaPairRDD<String, List<Integer>> customers = movieRatings.aggregateByKey(
  	    		new ArrayList<Integer>(), 
  	    		(listOfValues, value2) -> {
  	    			listOfValues.add(value2);
  	    			return listOfValues;
  	    		}, 
  	    		(list1, list2) -> {
  	    			list1.addAll(list2);
    				return list1;
  	    		}
  	    );
  	    
  	    
  	    JavaRDD<String> pairs = customers.flatMap((listOfValues) -> {
  	    	List<Integer> values = listOfValues._2;
  	    	List<String> edges = new ArrayList<>();
  	    	for (int startPair = 0; startPair < values.size(); startPair++) {
  	    		for (int endPair = startPair + 1; endPair < values.size(); endPair++) {
  	    			if (values.get(startPair) < values.get(endPair)) {
  	    				edges.add(values.get(startPair) + "," + values.get(endPair));
  	    			} else {
  	    				edges.add(values.get(endPair) + "," + values.get(startPair));
  	    			}
  	    		}
  	    	}
  	    	return edges.iterator();
  	    });
  	    
  	    JavaPairRDD<String, Integer> edges = pairs.mapToPair((edge) -> new Tuple2<>(edge, 1));
  	    JavaPairRDD<String, Integer> edgeCounts = edges.reduceByKey((i1, i2) -> i1 + i2);
  	    JavaPairRDD<String, Integer> sortedEdgeCounts = edgeCounts.sortByKey();
  	    
  	    List<String> manual = lines.collect();
  	    // List<Tuple2<String, Integer>> output = sortedEdgeCounts.collect();
  	    spark.stop();
  	    /*
  	    for (Tuple2<?,?> tuple : output) {
  	      System.out.println("(" + tuple._1() + ") " + tuple._2());
  	    }
		*/
  	   
  	   HashMap<String, List<Integer>> pairsM = new HashMap<String, List<Integer>>();
  	   for (String line : manual) {
  		   String key = line.split(DELIMITER)[0] + " " + line.split(DELIMITER)[2];
  		   if (!pairsM.containsKey(key)) {
  			 pairsM.put(key, new ArrayList<Integer>());
  		   }
  		   pairsM.get(key).add(Integer.parseInt(line.split(DELIMITER)[1]));
  	   }
  	   /*
  	   String[] _keys = (String[]) pairsM.keySet().toArray(new String[pairsM.size()]);
  	   Arrays.sort(_keys);
  	   for (String key : _keys) {
  		   System.out.println(key);
  		   System.out.println(pairsM.get(key).size());
  		   System.out.println(pairsM.get(key));
  	   }
  	   */
  	   
  	   
  	   HashMap<String, Integer> edgesM = new HashMap<String, Integer>();
  	   double totalUniqueEdges = 0;
  	   for (List<Integer> values : pairsM.values()) {
 	    	for (int startPair = 0; startPair < values.size(); startPair++) {
  	    		for (int endPair = startPair + 1; endPair < values.size(); endPair++) {
  	    			String key = null;
  	    			if (values.get(startPair) < values.get(endPair)) {
  	    				key = values.get(startPair) + "," + values.get(endPair);
  	    			} else {
  	    				key = values.get(endPair) + "," + values.get(startPair);
  	    			}
  	    			edgesM.put(key, edgesM.getOrDefault(key, 0) + 1);
  	    		}
  	    	}
 	    	totalUniqueEdges += (values.size() * (values.size() - 1)) / 2;
  	   }
  	   
  	   double totalEdges = 0;
  	   String[] keys = (String[]) edgesM.keySet().toArray(new String[edgesM.size()]);
  	   Arrays.sort(keys);
  	   List<String> nonOne = new ArrayList<>();
  	   for (String key : keys) {
  		   System.out.println("(" + key + ") " + edgesM.get(key));
  		   totalEdges++;
  		   if (edgesM.get(key) > 1) {
  			   nonOne.add(key);
  		   }
  	   }
  	   /*
  	   System.out.println("Keys that are above 1");
  	   double duplicates = 0;
  	   for (String key : nonOne) {
  		   System.out.println(key + " " + edgesM.get(key));
  		   duplicates += edgesM.get(key);
  	   }
  	   System.out.println("Total edges with more than one edge value: " + nonOne.size());
  	   System.out.println("Total duplicates " + (duplicates - nonOne.size()));
  	   System.out.println("Total unique edges (if all keys are unique): " + totalUniqueEdges);
  	   System.out.println("Total edges found: " + totalEdges);
  	   /*
  	    * 
  	    List<Tuple2<String, List<Integer>>> sorted = customers.collect();
  	    for (Tuple2<String,List<Integer>> tuple : sorted) {
  	      System.out.println(tuple._1());
  	      for (Integer value : tuple._2()) {
  	    	  System.out.print(value + " ");
  	      }
  	      System.out.println();
  	    }
  	    
  	    
  	    List<String> output1 = pairs.collect();

  	  
  	   
  	   List<Tuple2<String, Integer>> sortedPairs = edges.sortByKey().collect();
  	    
  	   System.out.println("Printing Pairs");
	   System.out.println(output1.size());
	   List<String> modifiable = new ArrayList<String>(output1);
	   Collections.sort(modifiable);
	   System.out.println("Starting to output");
	   for (String value : modifiable) {
	    	System.out.println(value);
	   }
  	   
  	   System.out.println("Printing edges");
  	   for (Tuple2<String, Integer> pair : sortedPairs) {
  		   System.out.println(pair._1() + " " + pair._2());
  	   }
  	   
  	   */
  	   
	}

}
