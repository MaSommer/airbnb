package airbnb;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Serializable;
import scala.Tuple2;



public class Program {	

	private static JavaRDD<String> listings_usRDD;
	private static JavaRDD<String> reviews_usRDD;
	private static JavaRDD<String> calendar_usRDD;

	public Program(JavaSparkContext sc){
		listings_usRDD = sc.textFile("target/listings_us.csv");
		reviews_usRDD = sc.textFile("target/reviews_us.csv");
		calendar_usRDD = sc.textFile("target/calendar_us.csv");
		task3();
	}

	
	public static void task2(){
		
		//b) Calculate number of distinct values for each field
		int numFields=0;
		
		
	}
	
	public static void task3(){
		String[] columndNeededListings = {"city", "price", "room_type", "reviews_per_month"};
		HelpMethods.mapAttributeAndIndex(listings_usRDD, 'l');
		JavaRDD<String[]> mappedListings = HelpMethods.mapToColumns(listings_usRDD, columndNeededListings, 'l');

		//avg booking price per night and per room type
		ArrayList<City> initialCityList = new ArrayList<City>();
		ArrayList<City> cityList = mappedListings.aggregate(initialCityList, HelpMethods.addAndCountCity(), HelpMethods.combineCity());
		
		printTask3(cityList);
		
//		String[] columndNeededCalendar = {"listing_id", "available"};
//		HelpMethods.mapAttributeAndIndex(calendar_usRDD, 'c');
//		JavaRDD<String[]> mappedCalendar = HelpMethods.mapToColumns(calendar_usRDD, columndNeededCalendar, 'c');
//		ArrayList<Listing> initialListingList = new ArrayList<Listing>();
//		ArrayList<Listing> listingList = mappedCalendar.aggregate(initialListingList, HelpMethods.addAndCountListing(), HelpMethods.combineListing());
//
//		//calculating top 3 hosts with the highest income
//		HelpMethods.calculateTop3HostsWithHighestIncomeForEachCity(cityList, listingList);
//		printTask4(cityList, listingList);
	}

	public static void task4(){
		String[] columndNeededListings = {"city", "price", "room_type", "reviews_per_month", "id", "host_id", "host_name", "host_total_listings_count"};
		HelpMethods.mapAttributeAndIndex(listings_usRDD, 'l');
		JavaPairRDD<String, String[]> listingPairs = HelpMethods.mapToPair(listings_usRDD, "id", columndNeededListings, HelpMethods.attributeListingIndex);

		String[] columndNeededCalendar = {"listing_id", "available"};
		HelpMethods.mapAttributeAndIndex(calendar_usRDD, 'c');
		JavaPairRDD<String, String[]> calendarPairs = HelpMethods.mapToPair(calendar_usRDD, "listing_id", columndNeededCalendar, HelpMethods.attributeCalendarIndex);

		//Using left outer join because have to make sure that all listings are included in the result
		JavaPairRDD<String, String[]> joinedPair = HelpMethods.lefOuterJoin(listingPairs, calendarPairs);
		
		new JavaPairRDD<String, String[]>(null, null, null);
		
//		joinedPair.red
		
//		joinedPair.combineByKey(createCombiner, mergeValue, mergeCombiners)
		
		joinedPair.foreach(new VoidFunction<Tuple2<String, String[]>>(){

			public void call(Tuple2<String, String[]> t) throws Exception {
				System.out.println(Arrays.toString(t._2));	
			}
			});;
		

	}

	public static void printTask3(ArrayList<City> cityList){
		int rooms = 0;
		DecimalFormat numberFormat = new DecimalFormat("#.00");
		for (City city : cityList) {
			System.out.println(city.getName());
			System.out.println("AvgPrice: $" + numberFormat.format(city.getAveragePricePerNight()) + " Avg nr. of reviews: " + numberFormat.format(city.getAvgNumberOfReviewsPerMonth()) 
					+ " Est. nr of nights: " + numberFormat.format(city.getEstimatedNumberOfNightsBookedPerYear()) + " Est. amount of money spent in a year: $" + numberFormat.format(city.getEstimatedAmountOfMoneySpentPerYear()));
			System.out.println("Avg. price per room type: ");
			for (RoomType roomType : city.getRoomTypeList()) {
				System.out.println(roomType.getRoomType() + ": $" + numberFormat.format(roomType.getAvgPricePerNight()));
			}
			rooms += city.getNumberOfRooms();
			System.out.println();
		}
		System.out.println("Number of rooms: " + rooms);
	}

	public static void printTask4(ArrayList<City> cityList, ArrayList<Listing> listingList){
		//4a and 4b
		int numberOfHosts = 0;
		int numberOfListings = 0;
		int numberOfHostsWithMoreThanOneListing = 0;
		for (City city : cityList) {
			for (Host host : city.getHostList()) {
				numberOfListings += host.getTotalListings();
				numberOfHosts++;
				if (host.getTotalListings() > 1){
					numberOfHostsWithMoreThanOneListing++;
				}
			}
		}
		double globalAverageNumberOfListingsPerHost = (double) numberOfListings/numberOfHosts;
		System.out.println("Global avg number of listings per host: " + globalAverageNumberOfListingsPerHost);
		double percentageOfHostsWithMoreThanOneListing = (double) numberOfHostsWithMoreThanOneListing/numberOfHosts;
		System.out.println("Percentage of hosts with more than 1 listings: " + percentageOfHostsWithMoreThanOneListing);
		for (City city : cityList) {
			System.out.println("Top 3 in " +city.getName() + ": ");
			int rank = 1;
			for (Host host : city.getTop3Hosts()) {
				if (host != null){
					System.out.println(rank +". Name: " + host.getHostName() + "\tID: " + host.getId() + "\t Income: $" + host.getTotalIncome());					
				}
				rank++;
			}
		}
	}

	//	public static void task4(){
	//		String[] columndNeeded = {"host_id", "host_listings_count"};
	//		JavaRDD<String[]> mappedListings = HelpMethods.mapToColumns(listings_usRDD, columndNeeded);
	//	}



	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
		.setAppName("AirbnbData")
		.setMaster("local[*]")
		;
		JavaSparkContext sc = new JavaSparkContext(conf);
		new Program(sc);
	}


}
