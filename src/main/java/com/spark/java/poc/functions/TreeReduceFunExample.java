package com.spark.java.poc.functions;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;

import com.spark.java.poc.utils.SparkUtils;

public class TreeReduceFunExample {
	
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "D:\\Unzip_Softwares\\winutils\\bin");
		JavaSparkContext sparkCxt = SparkUtils.createJavaSparkContext("TreeReduceFunExample");
		treeReduce(sparkCxt);
	}
	public static void treeReduce(JavaSparkContext sc) {
		JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(-5, -4, -3, -2, -1, 1, 2, 3, 4), 10);
		Function2<Integer, Integer, Integer> add = new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer a, Integer b) {
				return a + b;
			}
		};
		for (int depth = 1; depth <= 10; depth++) {
			int sum = rdd.treeReduce(add, depth);
			System.out.println("Sum ========"+ sum);
			assertEquals(-5, sum);
		}
	}
}
