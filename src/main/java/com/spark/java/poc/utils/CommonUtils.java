package com.spark.java.poc.utils;

public class CommonUtils {

	/**
	 * Below COMMA_DELIMITER help not split "," value which is have inside a String
	 * ex: "Sao Filipe, Fogo Island" 5675,"Sao Filipe Airport","Sao Filipe, Fogo
	 * Island","Cape
	 * Verde","SFL","GVSF",14.885,-24.48,617,-1,"U","Atlantic/Cape_Verde"
	 * 5674,"Praia International Airport","Praia, Santiago Island","Cape
	 * Verde","RAI","GVNP",14.9245,-23.4935,230,-1,"U","Atlantic/Cape_Verde"
	 * 
	 * @param line
	 * @return
	 */
	public static String[] splitByComma(String line) {
		return line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
	}

	/**
	 * Below method will split the row based on tab space seperated.
	 * 
	 * @param line
	 * @return
	 */
	public static String[] splitByTab(String line) {
		return line.split("\\t");
	}

	/**
	 * Below method will split the row based on tab space seperated and converts the String[] into Integer[].
	 * 
	 * @param line
	 * @return
	 */
	public static Integer[] splitByTabInteger(String line) {
		String str[] = line.split("\\t");
		Integer[] arr = new Integer[str.length];
		for (int i = 0; i < str.length; i++) {
			arr[i] = Integer.parseInt(str[i].trim());
		}
		return arr;
	}
	/*
	 * public static void main(String str[]) { CommonUtils utils= new CommonUtils();
	 * utils.
	 * splitByComma("1,\"Goroka\",\"Goroka\",\"Papua New, Guinea\",\"GKA\",\"AYGA\",-6.081689,145.391881,5282,10,\"U\",\"Pacific/Port_Moresby\""
	 * ); }
	 */
}
