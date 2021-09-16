package com.spark.java.poc.solutions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import com.spark.java.poc.utils.SparkUtils;

import scala.Tuple2;
/**
 * This Question is asked in IBM interview
 *  RDD1 = 1,2
 *  RDD2=3,4
 *  
 *  Need to get singe result RDD which have result as 4,5,5,6
 *  
 *  Logic to implement: 
 * 			We need to get all pair of both rdd and then add it.
 *  
 * 			Cartesian Transformation:
 * 			Return the Cartesian product of this RDD and another one, that is, the RDD of all pairs of elements (a, b) where a is in this and b is in other. 
 * 				i.e. (1,3),(1,4),(2,3),(2,4)
 * 	
 * @author kasho
 *
 */
public class IBMQuestionCombine2RDD {

	public static void main(String[] args) {
		JavaSparkContext sc= SparkUtils.createJavaSparkContext("IBMQuestionCombine2RDD");
		JavaRDD<Integer> list1 = sc.parallelize(Arrays.asList(1,2));
		JavaRDD<Integer> list2 = sc.parallelize(Arrays.asList(3,4));
		JavaPairRDD<Integer, Integer> pairRDD = list1.cartesian(list2);
		List<Integer> addedList = new ArrayList<Integer>();
		Function tupleFun =	new Function<Tuple2<Integer, Integer>, Integer>(){
			@Override
			public Integer call(Tuple2<Integer, Integer> pair) throws Exception {
				System.out.println("Pair Sum ---------"+pair._1()+pair._2());
				return pair._1()+pair._2();
			}
		};
		JavaRDD<Integer> addPair = pairRDD.map(tupleFun);
		List<Integer> data = addPair.collect();
		System.out.println(data);
	}
}

