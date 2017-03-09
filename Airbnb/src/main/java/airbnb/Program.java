package airbnb;

import java.awt.geom.Point2D;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import java.util.Scanner;

import javax.annotation.OverridingMethodsMustInvokeSuper;
import javax.swing.JFrame;
import javax.swing.SwingUtilities;
import javax.validation.OverridesAttribute;


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
	private static JavaRDD<String> neighborhood_test;
	

	public static int cityIndex;

	private static JavaRDD<String> neighbourHoodGeosjon;


	public Program(JavaSparkContext sc) throws IOException{
		listings_usRDD = sc.textFile("target/listings_us.csv");
		reviews_usRDD = sc.textFile("target/reviews_us.csv");
		calendar_usRDD = sc.textFile("target/calendar_us.csv");
		neighborhood_test = sc.textFile("neighborhood_test.csv");
		neighbourHoodGeosjon = sc.textFile("target/neighbourhoods.geojson");
//		task6();

	}


	
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
	
	public static void task2d(){
	 
		
		//b) Calculate number of distinct values for each field
		HelpMethods.mapAttributeAndIndex(listings_usRDD, 'l');
		String[] fieldList = {"country", "minimum_nights", "maximum_nights", "monthly_price", "price"};
		
		for (int i = 0; i < fieldList.length; i++) {
			String col = fieldList[0];
			HelpMethods.fieldListings(listings_usRDD,col);
		}
		
	}

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

			int[] keyIndexes = {0};
			JavaPairRDD<String, String[]> joinedPairWithCityAsKey = HelpMethods.mapToPairNewKey(joinedPairReducedByKey, keyIndexes);
			//{city, hostid_rank1, hostname, income}

			ArrayList<Host> initial = new ArrayList<Host>();
			JavaPairRDD<String, ArrayList<Host>> joinedPairAggregated = joinedPairWithCityAsKey.aggregateByKey(initial, HelpMethods.addAndCombineTop3Hosts(), HelpMethods.combinePairTop3Hosts());
			Print.task4c(joinedPairAggregated);
		}
	}

	public static void task5(){
		HelpMethods.mapAttributeAndIndex(listings_usRDD, 'l');
		HelpMethods.mapAttributeAndIndex(reviews_usRDD, 'r');
		Scanner sc = new Scanner(System.in);
		System.out.println("Which subtask do you want to run? (a or b)");
		String subtask = sc.next();


		if (subtask.equals("a")){
			String[] columndNeededListings = {"city", "price", "id"};
			String[] keys1 = {"id"};
			JavaPairRDD<String, String[]> listingPairs = HelpMethods.mapToPair(listings_usRDD, keys1, columndNeededListings, HelpMethods.attributeListingIndex);

			String[] columnsNeededReviews = {"listing_id", "reviewer_id", "reviewer_name"};
			String[] keys2 = {"listing_id"};
			JavaPairRDD<String, String[]> reviewerPairs = HelpMethods.mapToPair(reviews_usRDD, keys2, columnsNeededReviews, HelpMethods.attributeReviewIndex);
			JavaPairRDD<String, String[]> reviewerPairsWithNumberOfReviews = HelpMethods.mapToPairAddNumberOfReviewers(reviewerPairs);

			JavaPairRDD<String, String[]> joinedPair = HelpMethods.lefOuterJoin(reviewerPairsWithNumberOfReviews, listingPairs);
			//{listing_id, review_id, reviewer_name, number_of_reviewers_for_this_reviewer, city, price}

			int[] keyIndexes = {4, 1};
			//The following function is creating a new key for the JavaPairRDD. The new key is now "city reviewer_id".
			JavaPairRDD<String, String[]> joinedPairWithCityAndReviewerIdAsKey = HelpMethods.mapToPairNewKey(joinedPair, keyIndexes);

			JavaPairRDD<String, String[]> reducedPairOnKey = HelpMethods.reduceByKeySummingNumberOfReviews(joinedPairWithCityAndReviewerIdAsKey);
			//The next line of code change the key from cityAndReviewerId back to city
			JavaPairRDD<String, String[]> mappedCityAsKey = reducedPairOnKey.mapToPair(new PairFunction<Tuple2<String, String[]>, String, String[]>(){
				public Tuple2<String, String[]> call(Tuple2<String, String[]> t)throws Exception {
					return new Tuple2<String, String[]>(t._2[4], t._2);}});

			ArrayList<Reviewer> initial = new ArrayList<Reviewer>();
			JavaPairRDD<String, ArrayList<Reviewer>> joinedPairAggregatedOnCity = mappedCityAsKey.aggregateByKey(initial, HelpMethods.addAndCombineTop3Reviewers(), HelpMethods.combinePairTop3Reviewers());

			Print.task5a(joinedPairAggregatedOnCity);
		}
		else if (subtask.equals("b")){
			String[] columndNeededListings = {"city", "price", "id"};
			String[] keys1 = {"id"};
			JavaPairRDD<String, String[]> listingPairs = HelpMethods.mapToPair(listings_usRDD, keys1, columndNeededListings, HelpMethods.attributeListingIndex);

			String[] columnsNeededReviews = {"listing_id", "reviewer_id", "reviewer_name"};
			String[] keys2 = {"listing_id"};
			JavaPairRDD<String, String[]> reviewerPairs = HelpMethods.mapToPair(reviews_usRDD, keys2, columnsNeededReviews, HelpMethods.attributeReviewIndex);
			JavaPairRDD<String, String[]> joinedPair = HelpMethods.lefOuterJoin(reviewerPairs, listingPairs);

			JavaPairRDD<String, String[]> reviewerPairsWithNumberOfReviews = HelpMethods.mapToPairAddTotalAmountSpent(joinedPair);
			//{listing_id, review_id, reviewer_name, city, price, totalAmountSpent}

			int[] keyIndexes = {1};
			JavaPairRDD<String, String[]> joinedPairWithReviewerIdAsKey = HelpMethods.mapToPairNewKey(reviewerPairsWithNumberOfReviews, keyIndexes);

			//the following method reduces on key (reviewer_id) and sums up the total price
			JavaPairRDD<String, String[]> joinedPairReducedOnReviewerId = HelpMethods.reduceByKeySummingTotalAmountSpent(joinedPairWithReviewerIdAsKey);

			String[] initial = new String[6];
			initial[5] = "0.0"; 
			String[] result = joinedPairReducedOnReviewerId.aggregate(initial, HelpMethods.addAndCountTotalAmountSpent(), HelpMethods.combinePairTotalAmountSpent());
			Print.task5b(result);
		}
	}

	public static void task6() throws IOException{
		ArrayList<PolygonConstructor> polygons = HelpMethods.createPolygons();

	}

	public static void task6a() throws IOException {
		System.out.println("hei");
		HelpMethods.mapAttributeAndIndex(listings_usRDD, 'l');
		HelpMethods.mapAttributeAndIndex(neighborhood_test, 'a');
		
		String[] columns_listings = {"id","longitude","latitude"};
		JavaRDD<String[]> listingsRDD = HelpMethods.mapToColumns(listings_usRDD, columns_listings,'l');
		
		String[] columns_test = {"id","neighbourhood","city"};
		JavaRDD<String[]> testRDD = HelpMethods.mapToColumns(neighborhood_test, columns_test,'a');
		
		JavaRDD<String[]> neigRDD = listingsRDD.map(new Function<String[], String[]>() {

			public String[] call(String[] list) throws Exception {
				
				String[] result = new String[2];
				result[0] = list[0];
				
				ArrayList<PolygonConstructor> polygons = HelpMethods.createPolygons();
				String longitude = list[1];
				String latitude = list[2];
				
				
				
				for (int i = 0; i < polygons.size(); i++) {
					if(polygons.get(i).checkIfInsideOfPath(new Point2D.Double((HelpMethods.stringToDouble(longitude)),HelpMethods.stringToDouble(latitude)))) {
						result[1]=polygons.get(i).getNeighbourhood();
					}
				}
				return result;
				
			}
		});
		
		//Make neigRdd into JavaPairRDD
		JavaPairRDD<String,String[]> neigPairRDD = neigRDD.mapToPair(new PairFunction<String[], String, String[]>() {

			public Tuple2<String, String[]> call(String[] t) throws Exception {
				
				String[] neig = new String[1];
				neig[0] = t[1];
				Tuple2<String,String[]> result = new Tuple2<String, String[]>(t[0],neig);
				return result;
			}
			
		});
		JavaPairRDD<String,String[]> testPairRDD = testRDD.mapToPair(new PairFunction<String[], String, String[]>() {
			
			public Tuple2<String, String[]> call(String[] t) throws Exception {
				
				String[] neig = new String[1];
				neig[0] = t[1];
				Tuple2<String,String[]> result = new Tuple2<String, String[]>(t[0],neig);
				return result;
			}
			
		});

		
		JavaPairRDD<String, String[]> resultPairRDD = HelpMethods.lefOuterJoin(testPairRDD, neigPairRDD);
		
		
		JavaRDD<String[]> finalRestult = resultPairRDD.map(new Function<Tuple2<String,String[]>, String[]>() {

			public String[] call(Tuple2<String, String[]> v1) throws Exception {
				String[] listToCheck = v1._2;
				String match = "No";
				if (listToCheck[1].equals(listToCheck[3])) {
					match = "Yes";
				}
				String[] result = new String[3];
				
				result[0] = v1._1;
				result[1] = v1._2[1];
				result[2] = match;
				
				return result;
			}
		});
		finalRestult.foreach(new VoidFunction<String[]>() {
			
			public void call(String[] t) throws Exception {
				System.out.println(Arrays.toString(t));
				
			}
		});
		
		
	}




	public static void main(String[] args) throws IOException {
		SparkConf conf = new SparkConf()
		.setAppName("AirbnbData")
		.setMaster("local[*]")
		;
		JavaSparkContext sc = new JavaSparkContext(conf);

		Program p=new Program(sc);
		p.task6a();
	}
}
