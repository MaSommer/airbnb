package airbnb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import scala.Serializable;



public class Program {	

	private static JavaRDD<String> inputRDD;
	private static ArrayList<City> cityList;

	public Program(JavaSparkContext sc){
		inputRDD = sc.textFile("target/listings_us.csv");
		HelpMethods.mapAttributeAndIndex(inputRDD);
		avgBookingPricePerNight();
	}



	//TASK 3a
	public static void avgBookingPricePerNight(){
		String[] columndNeeded = {"city", "price"};
		JavaRDD<String[]> mapped = HelpMethods.mapToColumns(inputRDD, columndNeeded);
		
		ArrayList<City> initialCityList = new ArrayList<City>();
		
		ArrayList<City> cityList = mapped.aggregate(initialCityList, HelpMethods.addAndCount(), HelpMethods.combine());
		
		for (City city : cityList) {
			System.out.println(city.getName() + "\t\tNumberOfRooms: " + city.getNumberOfRooms() + "\t\tAvgPrice: " + city.getAveragePricePerNight());
		}
	}




	public static void main(String[] args) {
//		HelpMethods.priceAsDouble("$1,100.00");
		SparkConf conf = new SparkConf()
		.setAppName("AirbnbData")
		.setMaster("local[*]")
		;
		JavaSparkContext sc = new JavaSparkContext(conf);
		new Program(sc);
	}


}
