package com.spark.java.poc.solutions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import com.spark.java.poc.utils.SparkUtils;

import scala.Tuple2;

public class IBMQuestionWordCountOfEachLineRDD implements Serializable {

	public static void main(String[] args) {
		JavaRDD<String> strRDD = SparkUtils.readTextFile("IBMQuestionWordCountOfEachLine", "wordCount.txt",1);
		
		// Load our input data.
		/**
		JavaRDD<String> lineRDD = strRDD.map(new Function<String, String>(){
			int i=1;
			@Override
			public String call(String line) throws Exception {
				System.out.println("Inside Map ---- "+i+"---"+ Arrays.toString(line.split("\n")));
				i++;
				return Arrays.toString(line.split("\n")); 
			}
			
		});
		**/
		JavaRDD<String> lineArrayRDD = strRDD.flatMap(new FlatMapFunction<String, String>() {
			int i=1;
			public Iterator<String> call(String x) throws Exception {
				List<String> wordArry= Arrays.asList(x.split(" "));
				System.out.println("Inside flatMap ---- "+i+"---"+ wordArry.toString());
				i++;
				return  Arrays.asList(x.split(" ")).iterator();
			}
		});
		JavaPairRDD<String, Integer> wordOccurance= lineArrayRDD.mapToPair(new PairFunction<String, String,Integer>(){
			int i=1;
			@Override
			public Tuple2<String, Integer> call(String word) throws Exception {
				//System.out.println("Inside mapToPair ----- "+ word);
				System.out.println("---- Inside mapToPair -- "+ i);
				i++;
				return new Tuple2(word,1);
			}
			
		});

		List<Tuple2<String, Integer>> data = wordOccurance.collect();
		System.out.println(data);
		System.exit(0);
		//System.out.println(wordOccurance.getNumPartitions());
	JavaPairRDD<String, Integer> addByKey = wordOccurance.reduceByKey(new Function2<Integer, Integer, Integer>(){
    			public Integer call(Integer x, Integer y){ 
    				//System.out.println("Inside reduceByKey --X"+ x + "-- Y ---"+y );
    				return x + y;
    			}
	});
			
	JavaPairRDD<Integer,String> lineWordCount =	addByKey.mapToPair(new PairFunction<Tuple2<String,Integer>, Integer, String>() {
//
		@Override
		public Tuple2<Integer, String> call(Tuple2<String, Integer> keyValue) throws Exception {
			return keyValue.swap();
		}
		
	});
		
	System.out.println(lineWordCount.sortByKey(false).first());
	Tuple2<Integer, String> topWordOfLine =lineWordCount.sortByKey(false).first();
	System.out.println("Word -- "+ topWordOfLine._2+ " ---Occurence -- "+topWordOfLine._1);
	
	/**
	JavaRDD<String[]> wordArrayRDD = lineArrayRDD.map(new Function<String, String>() {
		int i=1;
		public String[] call(List<String> x) throws Exception {
			System.out.println("Inside flatMap ---- "+i+"---"+ x.toString());
			i++;
			return new String[10];
		}
	});
		**/
	/**
	wordArrayRDD.mapToPair(new PairFunction<String[], String, Integer>(){
//
		@Override
		public Tuple2<String, Integer> call(String[] t) throws Exception {
			return null;
		}
		
	});
		JavaPairRDD<String, Integer> wordCountRDD =  wordArrayRDD.mapPartitionsToPair(new PairFlatMapFunction<Iterator<String[]>, String, Integer>() {

			@Override
			public Iterator<Tuple2<String, Integer>> call(Iterator<String[]> strArray) throws Exception {
				List<Tuple2<String, Integer>> result = new ArrayList<Tuple2<String, Integer>>();
				for(String str : strArray.next()) {
					System.out.println("Inside mapPartitionsToPair ---- "+str.toString());
					result.add(new Tuple2(str, 1));
				}
				return result.iterator();
			}
		});
		 //JavaPairRDD<String, Integer> eachWordCountArray = 
		wordArrayRDD.mapToPair(new PairFunction<String [], String, Integer>(){

			@Override
			public Tuple2<String, Integer> call(String[] x) throws Exception {
				return new Tuple2(x, 1);
			}
			
		});
		**/
		/**
		JavaRDD<String[]> lineArrayRDD = strRDD.map(new Function<String, String[]>() {
			public String[] call(String x) throws Exception {
				return x.split("\n");
			}

		});
		
		
		
		
		System.out.println("Total Lines  ----------" +lineArrayRDD.count());
		JavaPairRDD<String, String[]> Line = lineArrayRDD.mapToPair(new PairFunction<String, String, String[]>() {
			public Tuple2<String, String[]> call(String x) {
				return new Tuple2(x, x.split(" "));
			}
		});
			**/
		//List<Tuple2<String, Integer>> data = wordOccurance.collect();
		int i = 1;
		int j=1;
		/*
		 * for (String str: data) {
		 * //System.out.println("Line -- "+j+" --- "+Arrays.toString(lineWords));
		 * //System.out.println("Line -- "+j+" --- "+lineWords._1+"---"+lineWords._2);
		 * //System.out.println(str); j++; }
		 */

	}
}
