package com.spark.java.poc.solutions;

import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import com.spark.java.poc.utils.SparkUtils;

import scala.Tuple2;

public class StockMaxOpeningPrice {

	public static void main(String[] args) {
		JavaRDD<String> rddRows = SparkUtils.readTextFile("StockMaxOpeningPrice", "Stock_data.csv", 1);
		JavaPairRDD<String,String> pairRddRows = rddRows.mapToPair(new PairFunction<String, String, String>() {
			@Override
			public Tuple2<String, String> call(String row) throws Exception {
				String stockArry[]= row.split(",");
				String stock_id=stockArry[0];
				String stock_open_price=stockArry[1];
				return new Tuple2(stock_id,stock_open_price);
			}
		});
		JavaPairRDD<String,String> maxPriceStockId = pairRddRows.reduceByKey(new Function2<String, String, String>() {
			
			@Override
			public String call(String v1, String v2) throws Exception {
				Integer.parseInt(v1);
				Integer.parseInt(v2);
				String maxOpenPrice;
				if(Integer.parseInt(v1) >Integer.parseInt(v2)) {
					maxOpenPrice=v1;
				} else {
					maxOpenPrice=v2;
				}
				return maxOpenPrice;
			}
		});
		
		List<Tuple2<String,String>> stockId= maxPriceStockId.collect();
		
		for(Tuple2 element: stockId) {
			System.out.println("Stock_Id ---" + element._1()+ " ---- Stock_Open_Price Max--- "+element._2());
		}
		
	}

}
