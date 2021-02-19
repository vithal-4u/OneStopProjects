package com.spark.java.poc.udf;

import org.apache.spark.sql.api.java.UDF6;

public class DataValidationUDF {

	// Create udf to prefix 00 in single column (UDF1 is used)
	public static UDF6<String, String, String, String, String, String, String> formatId = new UDF6<String, String, String, String, String, String, String>() {

		@Override
		public String call(String t1, String t2, String t3, String t4, String t5, String t6) throws Exception {

			return null;
		}

	};
}
