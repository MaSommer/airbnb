package airbnb;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
<<<<<<< HEAD
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
=======
import java.util.Scanner;

import javax.annotation.OverridingMethodsMustInvokeSuper;
import javax.validation.OverridesAttribute;
>>>>>>> master

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.RowFactory;

import scala.Serializable;
import scala.Tuple2;



public class Program {	

	private static JavaRDD<String> listings_usRDD;
	private static JavaRDD<String> reviews_usRDD;
	private static JavaRDD<String> calendar_usRDD;
	public static int cityIndex;

	public Program(JavaSparkContext sc){
		listings_usRDD = sc.textFile("target/listings_us.csv");
		reviews_usRDD = sc.textFile("target/reviews_us.csv");
		calendar_usRDD = sc.textFile("target/calendar_us.csv");
//		task3();
	}

<<<<<<< HEAD
	
	public static void task2b(){
		
		HashMap<String,Integer> result = new HashMap<String, Integer>(); 
				
		//b) Calculate number of distinct values for each field
		HelpMethods.mapAttributeAndIndex(listings_usRDD, 'l');
		String[] headerList = listings_usRDD.first().split("\t");
		
		for (int i = 0; i < headerList.length; i++) {
			String col = headerList[i];
			
			try {
				JavaRDD<String> ret = HelpMethods.mapToColumnsString(listings_usRDD, col, 'l').distinct();				
				int num = (int) ret.count();
				result.put(col,num );
			} 
			catch(Exception e) {
				continue;
			}
		}
		
		List<String> keys = new ArrayList(result.keySet());
		Collections.sort(keys);
		
		for (String s : keys) {
			System.out.println(s + " has: " + result.get(s) + " distinct fields");
		}
		
	}

	//c) Listings from how many and which cities are contained in the dataset
	public static void task2c(){
	 
		
		HelpMethods.mapAttributeAndIndex(listings_usRDD, 'l');
		String[] headerList = listings_usRDD.first().split("\t");
		 
		
		for (int i = 0; i < headerList.length; i++) {
			if (headerList[i].equals("city")) {
				cityIndex = i;
			}
		}
		
		try {
			JavaRDD<String> ret = HelpMethods.mapToColumnsString(listings_usRDD, headerList[cityIndex], 'l').distinct();				
			int num = (int) ret.count();
			ret.foreach(new VoidFunction<String>() {

				public void call(String t) throws Exception {
					if(t.equals("city")) {
						
					}
					else {
						System.out.println(t);						
					}
				}
				
			});
			System.out.println(num-1);
		} 
		catch(Exception e) {
			System.out.println("Unable to run");
		}
		
}
	
	
	
	
=======

	public static void task3(){
		HelpMethods.mapAttributeAndIndex(listings_usRDD, 'l');
		Scanner sc = new Scanner(System.in);
		System.out.println("Which subtask do you want to run? (a, b, c, d or e)");
		String subtask = sc.next();
		if (subtask.equals("a")){
			String[] columndNeededListingsa = {"city", "price"};
			String[] keysa = {"city"};
			JavaPairRDD<String, String[]> mappedListingsPair = HelpMethods.mapToPair(listings_usRDD, keysa, columndNeededListingsa, HelpMethods.attributeListingIndex);

			String[] initial = new String[3];
			JavaPairRDD<String, String[]> mappedListingsPairAggregated = mappedListingsPair.aggregateByKey(initial, HelpMethods.addAndCombinePairAverage(), HelpMethods.combinePairAverage());
			Print.task3a(mappedListingsPairAggregated);
		}
		else if (subtask.equals("b")){
			String[] columndNeededListingsb = {"city", "price"};
			String[] keysb = {"city", "room_type"};
			JavaPairRDD<String, String[]> mappedListingsPair = HelpMethods.mapToPair(listings_usRDD, keysb, columndNeededListingsb, HelpMethods.attributeListingIndex);

			String[] initial = new String[3];
			JavaPairRDD<String, String[]> mappedListingsPairAggregated = mappedListingsPair.aggregateByKey(initial, HelpMethods.addAndCombinePairAverage(), HelpMethods.combinePairAverage());
			Print.task3b(mappedListingsPairAggregated);
		}
		else if (subtask.equals("c")){
			String[] columndNeededListingsc = {"city", "reviews_per_month"};
			String[] keysc = {"city"};
			JavaPairRDD<String, String[]> mappedListingsPair = HelpMethods.mapToPair(listings_usRDD, keysc, columndNeededListingsc, HelpMethods.attributeListingIndex);

			String[] initial = new String[3];
			JavaPairRDD<String, String[]> mappedListingsPairAggregated = mappedListingsPair.aggregateByKey(initial, HelpMethods.addAndCombinePairAverage(), HelpMethods.combinePairAverage());
			Print.task3c(mappedListingsPairAggregated);
		}
		else if (subtask.equals("d")){
			String[] columndNeededListingsd = {"city", "reviews_per_month"};
			String[] keysd = {"city"};
			JavaPairRDD<String, String[]> mappedListingsPair = HelpMethods.mapToPair(listings_usRDD, keysd, columndNeededListingsd, HelpMethods.attributeListingIndex);

			String[] initial = new String[3];
			JavaPairRDD<String, String[]> mappedListingsPairAggregated = mappedListingsPair.aggregateByKey(initial, HelpMethods.addAndCombineEstimatedNumberOfNights(), HelpMethods.combinePairEstimatedNumberOfNights());
			Print.task3d(mappedListingsPairAggregated);
		}
		else if (subtask.equals("e")){
			String[] columndNeededListingse = {"city", "reviews_per_month", "price"};
			String[] keyse = {"city"};
			JavaPairRDD<String, String[]> mappedListingsPair = HelpMethods.mapToPair(listings_usRDD, keyse, columndNeededListingse, HelpMethods.attributeListingIndex);

			String[] initial = new String[4];
			JavaPairRDD<String, String[]> mappedListingsPairAggregated = mappedListingsPair.aggregateByKey(initial, HelpMethods.addAndCombineAmountOfMoneySpent(), HelpMethods.combinePairAmountOfMoneySpent());
			Print.task3e(mappedListingsPairAggregated);
		}
	}

	public static void task4(){
		HelpMethods.mapAttributeAndIndex(listings_usRDD, 'l');
		HelpMethods.mapAttributeAndIndex(calendar_usRDD, 'c');
		Scanner sc = new Scanner(System.in);
		System.out.println("Which subtask do you want to run? (a, b or c)");
		String subtask = sc.next();
		if (subtask.equals("a") || subtask.equals("b")){
			String[] columndNeededListings = {"host_id", "host_total_listings_count"};
			String[] keysListing = {"host_id"};
			JavaPairRDD<String, String[]> listingPairs = HelpMethods.mapToPair(listings_usRDD, keysListing, columndNeededListings, HelpMethods.attributeListingIndex);
			JavaPairRDD<String, String[]> listingPairsReduced = HelpMethods.reduceByKeyOnHostId(listingPairs);
			
//			listingPairsReduced.foreach(new VoidFunction<Tuple2<String, String[]>>(){
//				
//							public void call(Tuple2<String, String[]> t) throws Exception {
//								System.out.println(Arrays.toString(t._2));	
//							}
//						});;
			
			//{total listings, total number of hosts, hosts with more than 1 listing}
			double[] initial = {0.0, 0.0, 0.0};
			double[] totalAndNumberOfListings = listingPairsReduced.aggregate(initial, HelpMethods.addAndCombineAverageNumberOfListings(), HelpMethods.combinePairAverageNumberOfListings());
			Print.task4ab(totalAndNumberOfListings);
		}
		else if (subtask.equals("c")){
			String[] columndNeededListings = {"city", "price", "id", "host_id", "host_name", "host_total_listings_count"};
			HelpMethods.mapAttributeAndIndex(listings_usRDD, 'l');
			String[] keys1 = {"id"};
			JavaPairRDD<String, String[]> listingPairs = HelpMethods.mapToPair(listings_usRDD, keys1, columndNeededListings, HelpMethods.attributeListingIndex);
			
			String[] columndNeededCalendar = {"listing_id", "available"};
			HelpMethods.mapAttributeAndIndex(calendar_usRDD, 'c');
			String[] keys2 = {"listing_id"};
			JavaPairRDD<String, String[]> calendarPairs = HelpMethods.mapToPair(calendar_usRDD, keys2, columndNeededCalendar, HelpMethods.attributeCalendarIndex);
			
			//Using left outer join because have to make sure that all listings are included in the result
			JavaPairRDD<String, String[]> joinedPair = HelpMethods.lefOuterJoin(listingPairs, calendarPairs);
			
			JavaPairRDD<String, String[]> joinedPairReducedByKey = HelpMethods.reduceByKeyOnAvailable(joinedPair);
			
			JavaPairRDD<String, String[]> joinedPairWithCityAsKey = HelpMethods.mapToPairNewKey(joinedPairReducedByKey, 0);
			//{city, hostid_rank1, hostname, income}
			
			ArrayList<Host> initial = new ArrayList<Host>();
			JavaPairRDD<String, ArrayList<Host>> joinedPairAggregated = joinedPairWithCityAsKey.aggregateByKey(initial, HelpMethods.addAndCombineTop3Hosts(), HelpMethods.combinePairTop3Hosts());
			Print.task4c(joinedPairAggregated);
		}
	}

	public static void task5(){

		//		String[] columnsNeededListings = {"city", "price", "id"};
		//		HelpMethods.mapAttributeAndIndex(listings_usRDD, 'l');
		//		JavaPairRDD<String, String[]> listingPairs = HelpMethods.mapToPair(listings_usRDD, "id", columnsNeededListings, HelpMethods.attributeListingIndex);
		//		
		//		String[] columnsNeededReviews = {"listing_id", "reviewer_id", "reviewer_name"};
		//		HelpMethods.mapAttributeAndIndex(reviews_usRDD, 'r');
		//		JavaPairRDD<String, String[]> reviewerPairs = HelpMethods.mapToPair(reviews_usRDD, "listing_id", columnsNeededReviews, HelpMethods.attributeReviewIndex);
		//		
		//		JavaPairRDD<String, String[]> joinedPair = HelpMethods.lefOuterJoin(reviewerPairs, listingPairs);
		//		
		//		ArrayList<City> initialCityList = new ArrayList<City>();
		//		ArrayList<City> cityList = joinedPair.aggregate(initialCityList, HelpMethods.addAndCountCityAndReviewers(), HelpMethods.combineCityLists());
		//		printTask5(cityList);
		//		joinedPair.foreach(new VoidFunction<Tuple2<String, String[]>>(){
		//						public void call(Tuple2<String, String[]> t) throws Exception {
		//							System.out.println(Arrays.toString(t._2));	
		//						}
		//					});;

	}

>>>>>>> master


	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
		.setAppName("AirbnbData")
		.setMaster("local[*]")
		;
		JavaSparkContext sc = new JavaSparkContext(conf);
<<<<<<< HEAD
		Program p=new Program(sc);
		p.task2c();
=======

		new Program(sc);
>>>>>>> master
	}


}
