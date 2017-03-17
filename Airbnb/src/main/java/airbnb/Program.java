package airbnb;

import java.awt.geom.Point2D;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
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

import com.couchbase.client.core.event.consumers.LoggingConsumer.OutputFormat;

import scala.Serializable;
import scala.Tuple2;



public class Program {	

	private static JavaRDD<String> listings_usRDD;
	private static JavaRDD<String> reviews_usRDD;
	private static JavaRDD<String> calendar_usRDD;
	private static JavaRDD<String> neighbourHoodGeosjon;
	private static JavaRDD<String> neighbourHoodTest_usRDD;

	public Program(JavaSparkContext sc) throws IOException{
		listings_usRDD = sc.textFile("target/listings_us.csv");
		reviews_usRDD = sc.textFile("target/reviews_us.csv");
		calendar_usRDD = sc.textFile("target/calendar_us.csv");
		neighbourHoodGeosjon = sc.textFile("target/neighbourhoods.geojson");
		neighbourHoodTest_usRDD = sc.textFile("target/neighborhood_test.csv");
		task4();
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
			HelpMethods.mapToPairToStringAvg(mappedListingsPairAggregated).coalesce(1).saveAsTextFile("target/task3a");
		}
		else if (subtask.equals("b")){
			String[] columndNeededListingsb = {"city", "price"};
			String[] keysb = {"city", "room_type"};
			System.out.println(listings_usRDD.first());
			JavaPairRDD<String, String[]> mappedListingsPair = HelpMethods.mapToPair(listings_usRDD, keysb, columndNeededListingsb, HelpMethods.attributeListingIndex);

			String[] initial = new String[3];
			JavaPairRDD<String, String[]> mappedListingsPairAggregated = mappedListingsPair.aggregateByKey(initial, HelpMethods.addAndCombinePairAverage(), HelpMethods.combinePairAverage());
			Print.task3b(mappedListingsPairAggregated);
			HelpMethods.mapToPairToStringAvg(mappedListingsPairAggregated).coalesce(1).saveAsTextFile("target/task3b");
		}
		else if (subtask.equals("c")){
			String[] columndNeededListingsc = {"city", "reviews_per_month"};
			String[] keysc = {"city"};
			JavaPairRDD<String, String[]> mappedListingsPair = HelpMethods.mapToPair(listings_usRDD, keysc, columndNeededListingsc, HelpMethods.attributeListingIndex);

			String[] initial = new String[3];
			JavaPairRDD<String, String[]> mappedListingsPairAggregated = mappedListingsPair.aggregateByKey(initial, HelpMethods.addAndCombinePairAverage(), HelpMethods.combinePairAverage());
			Print.task3c(mappedListingsPairAggregated);
			HelpMethods.mapToPairToStringAvg(mappedListingsPairAggregated).coalesce(1).saveAsTextFile("target/task3c");
		}
		else if (subtask.equals("d")){
			String[] columndNeededListingsd = {"city", "reviews_per_month"};
			String[] keysd = {"city"};
			JavaPairRDD<String, String[]> mappedListingsPair = HelpMethods.mapToPair(listings_usRDD, keysd, columndNeededListingsd, HelpMethods.attributeListingIndex);

			String[] initial = new String[3];
			JavaPairRDD<String, String[]> mappedListingsPairAggregated = mappedListingsPair.aggregateByKey(initial, HelpMethods.addAndCombineEstimatedNumberOfNights(), HelpMethods.combinePairEstimatedNumberOfNights());
			Print.task3d(mappedListingsPairAggregated);
			HelpMethods.mapToPairToString(mappedListingsPairAggregated).coalesce(1).saveAsTextFile("target/task3d");
		}
		else if (subtask.equals("e")){
			String[] columndNeededListingse = {"city", "reviews_per_month", "price"};
			String[] keyse = {"city"};
			JavaPairRDD<String, String[]> mappedListingsPair = HelpMethods.mapToPair(listings_usRDD, keyse, columndNeededListingse, HelpMethods.attributeListingIndex);

			String[] initial = new String[4];
			JavaPairRDD<String, String[]> mappedListingsPairAggregated = mappedListingsPair.aggregateByKey(initial, HelpMethods.addAndCombineAmountOfMoneySpent(), HelpMethods.combinePairAmountOfMoneySpent());
			Print.task3e(mappedListingsPairAggregated);
			HelpMethods.mapToPairToString(mappedListingsPairAggregated).coalesce(1).saveAsTextFile("target/task3e");
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

			//Using left outer join because we have to make sure that all listings are included in the result
			JavaPairRDD<String, String[]> joinedPair = HelpMethods.lefOuterJoin(listingPairs, calendarPairs);

			JavaPairRDD<String, String[]> joinedPairReducedByKey = HelpMethods.reduceByKeyOnAvailable(joinedPair);
			
			int[] keyIndexes1 = {0, 3};
			JavaPairRDD<String, String[]> joinedPairWithHostIdAndCityAsKey = HelpMethods.mapToPairNewKey(joinedPairReducedByKey, keyIndexes1);
			System.out.println("here");
			System.out.println(Arrays.toString(joinedPairWithHostIdAndCityAsKey.first()._2));
			//The next line sums up total income and leave the tupple {city, host_id, host_name, totalIncome}
			JavaPairRDD<String, String[]> joinedPairWithHostIdAndCityAsKeySummingIncome = HelpMethods.mapToPairNewKeyStringArray(joinedPairWithHostIdAndCityAsKey);
			//The next line reduce by key and summing total income for each host in each city
			JavaPairRDD<String, String[]> joinedPairWithHostIdAsKeyAndCityReduced = HelpMethods.reduceByKeySummingTotalIncome(joinedPairWithHostIdAndCityAsKeySummingIncome);
			
			int[] keyIndexes = {0};
			//{city, host_id, host_name, totalIncome}
			JavaPairRDD<String, String[]> joinedPairWithCityAsKey = HelpMethods.mapToPairNewKey(joinedPairWithHostIdAsKeyAndCityReduced, keyIndexes);
			
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
			HelpMethods.mapToPairToStringFromReviewList(joinedPairAggregatedOnCity).coalesce(1).saveAsTextFile("target/task5a");
			
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
	
	public static void task6a() throws IOException{
		ArrayList<PolygonConstructor> polygons = HelpMethods.createPolygons();
		HelpMethods.mapAttributeAndIndex(listings_usRDD, 'l');
		String[] columndNeededListings = {"id", "longitude", "latitude"};
		String[] keys1 = {"id"};
		JavaPairRDD<String, String[]> listingPairs = HelpMethods.mapToPair(listings_usRDD, keys1, columndNeededListings, HelpMethods.attributeListingIndex);
		JavaPairRDD<String, String[]> listingPairsWithNeighborhood = HelpMethods.mapToPairAddNeighborhood(listingPairs, polygons);
		
		HelpMethods.mapAttributeAndIndex(neighbourHoodTest_usRDD, 'n');
		String[] columndNeededNeigh = {"id", "neighbourhood"};
		String[] keys2 = {"id"};
		JavaPairRDD<String, String[]> neighPairs = HelpMethods.mapToPair(neighbourHoodTest_usRDD, keys2, columndNeededNeigh, HelpMethods.attributeNeihbourhoodIndex);
		
		JavaPairRDD<String, String[]> joinedPairRDD = HelpMethods.lefOuterJoin(neighPairs, listingPairsWithNeighborhood);
		
		//number of listings, number of neighbourhood that is equal
		double[] initial = {0.0, 0.0};
		double[] result = joinedPairRDD.aggregate(initial, HelpMethods.addAndCountNumberOfEqualNeighborhood(), HelpMethods.combinePairEqualNeighbourhood());
		Print.task6a(result);
		
	}
	
	public static void task6b() throws IOException{
		ArrayList<PolygonConstructor> polygons = HelpMethods.createPolygons();
		HelpMethods.mapAttributeAndIndex(listings_usRDD, 'l');
		String[] columndNeededListings = {"id", "amenities", "longitude", "latitude"};
		String[] keys1 = {"id"};
		JavaPairRDD<String, String[]> listingPairs = HelpMethods.mapToPair(listings_usRDD, keys1, columndNeededListings, HelpMethods.attributeListingIndex);
		// {"id", "amenities", "longtitude", "latitude", "neighbourhood"}
		JavaPairRDD<String, String[]> listingPairsMapedToNeighbourhood = HelpMethods.mapToPairAddNeighbourhood(listingPairs, polygons);
		
		//Makes neighbourhood new key
		int[] keyIndexes = {4};
		JavaPairRDD<String, String[]> listingPairsMapedToNeighbourhoodNewKey = HelpMethods.mapToPairNewKey(listingPairsMapedToNeighbourhood, keyIndexes);
		
		ArrayList<String> initial = new ArrayList<String>();
		JavaPairRDD<String, ArrayList<String>> results = listingPairsMapedToNeighbourhoodNewKey.aggregateByKey(initial, HelpMethods.addAndCountDistinctAmenities(), HelpMethods.combinePairDistinctAmenities());
		Print.task6b(results);
		HelpMethods.mapToPairToStringArray(results).coalesce(1).saveAsTextFile("target/task6b");
	}

//	public static void task7(){
//		final String[] columndNeededListingsa = {"city", "price"};
//		String[] keysa = {"city"};
//		JavaRDD<String> listings =  listings_usRDD.map(new Function<String, String>() {
//
//			public String call(String v1) throws Exception {
//				String 
//				return null;
//			}
//		});
//		JavaPairRDD<String, String> mappedListingsPair = HelpMethods.mapToPair(listings_usRDD, keysa, columndNeededListingsa, HelpMethods.attributeListingIndex);
//		JavaPairRDD<String, String> output = HelpMethods.mapToPairToString(mappedListingsPair);
//	}


	public static void main(String[] args) throws IOException {
		SparkConf conf = new SparkConf()
		.setAppName("AirbnbData")
		.setMaster("local[*]")
		;
		JavaSparkContext sc = new JavaSparkContext(conf);

		new Program(sc);
	}


}
