package alternative_listings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.spark_project.jetty.http.HttpGenerator.Result;

import scala.Serializable;
import scala.Tuple2;
import airbnb.HelpMethods;


public class Program implements Serializable{
	
	private String listing_id;
	private String price;
	private String room_type;
	private int topN;
	private String date;
	private String acceptable_price;
	private String acceptableDistance;
	
	private String[] mainTuple;
	
	private static JavaRDD<String> listings_usRDD;
	private static JavaRDD<String> calendar_usRDD;
	
	private JavaPairRDD<String, String[]> listingsConutCommonAmenities;
	
	public Program(JavaSparkContext sc, String listing_id, String date, String acceptable_price, String acceptableDistance, String topN){
		this.listing_id = listing_id;
		this.date = date;
		this.acceptable_price = acceptable_price;
//		this.acceptable_price = ""+ HelpMethods.stringToDouble(acceptable_price)/(double)100;
		this.acceptableDistance = acceptableDistance;
		this.topN = Integer.parseInt(topN);
		init(sc);
		run();
//		visualization();
	}
	
	public void init(JavaSparkContext sc){
		listings_usRDD = sc.textFile("target/listings_us.csv");
		calendar_usRDD = sc.textFile("target/calendar_us.csv");
		HelpMethods.mapAttributeAndIndex(listings_usRDD, 'l');
		HelpMethods.mapAttributeAndIndex(calendar_usRDD, 'c');
	}
	
	public void run(){
		String[] columndNeededListings = {"id", "price", "room_type", "latitude", "longitude", "amenities", "name", "review_scores_rating", "picture_url"};
		String[] keysListing = {"id"};
		JavaPairRDD<String, String[]> mappedListingsPair = HelpMethods.mapToPair(listings_usRDD, keysListing, columndNeededListings, HelpMethods.attributeListingIndex);
//		finds the roomtype, latitude, longitude and amenities for listing_id
		String[] room_type_price_lat_long_amneties = HelpMethods.filterOutOnListingIdFindingRoomTypeAndPrice(mappedListingsPair, listing_id);
		mainTuple = HelpMethods.findMainTuple(mappedListingsPair, listing_id);
		room_type = room_type_price_lat_long_amneties[0];
		price = room_type_price_lat_long_amneties[1];
		String mainLat = room_type_price_lat_long_amneties[2];
		String mainLong = room_type_price_lat_long_amneties[3];
		String amenities = room_type_price_lat_long_amneties[4];
		String amenitiesReduced = amenities.substring(2, amenities.length()-2);
		String[] amenitiesList = amenitiesReduced.split(",");
				
		String[] columndNeededCalendar = {"available", "date", "listing_id"};
		String[] keysCalendar = {"listing_id"};
		JavaPairRDD<String, String[]> mappedCalendarPair = HelpMethods.mapToPair(calendar_usRDD, keysCalendar, columndNeededCalendar, HelpMethods.attributeCalendarIndex);
		
		//Filter out wrong date and not avialable
		JavaPairRDD<String, String[]> mappedCalendarPairFiltered = HelpMethods.filterOutOnDateAndAvailable(mappedCalendarPair, date);
		
		JavaPairRDD<String, String[]> joinedPair = Join.join(mappedListingsPair, mappedCalendarPairFiltered);
		//This methods only return the rows goes through the constriaints about room_type, price, distance
		JavaPairRDD<String, String[]> filteredOnDateAndAvailable = HelpMethods.filterOutOnRoomTypePriceAndDistance(joinedPair, listing_id, date, room_type, price, acceptable_price, acceptableDistance, mainLat, mainLong);
		this.listingsConutCommonAmenities = Map.mapToRankByAmenities(filteredOnDateAndAvailable, amenitiesList);
		
		//Maps every row to have the same key, this will make the aggregate function easier. 
		JavaPairRDD<String, String[]> listingsSortedOnCommoneAmenitiesCommonKey = Map.mapToCommonKey(listingsConutCommonAmenities, mainLat, mainLong);

		ArrayList<String[]> initial = new ArrayList<String[]>();
		JavaPairRDD<String, ArrayList<String[]>> topNList = listingsSortedOnCommoneAmenitiesCommonKey.aggregateByKey(initial, Aggreagate.addAndCombineTopNListingsByKey(), Aggreagate.combinePairTopNListings(topN));
		
		JavaRDD<String> result = Map.mapToStringPrintQueue(topNList);
		
		result.coalesce(1).saveAsTextFile("target/alternatives");

	}
	
	public void visualization(){
		JavaRDD<String> ouputVisualization = Map.mapToStringVisualisation(listingsConutCommonAmenities);
		ouputVisualization.coalesce(1).saveAsTextFile("target/visualization");	
	}
	
	
	public static void main(String[] args) throws IOException {
		SparkConf conf = new SparkConf()
		.setAppName("AirbnbData")
		.setMaster("local[*]")
		;
		JavaSparkContext sc = new JavaSparkContext(conf);
		String listing_id = "4717459";
		String date = "2016-12-15";
		String acceptable_price = "30";
		//radius constraint in km
		String radiusConstraint = "10";
		String topN = "40";
	
		Program p= new Program(sc, listing_id, date, acceptable_price, radiusConstraint, topN);
	}

}
