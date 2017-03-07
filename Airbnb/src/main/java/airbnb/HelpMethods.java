package airbnb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class HelpMethods {

	public static HashMap<String, Integer> attributeListingIndex;
	public static HashMap<String, Integer> attributeCalendarIndex;
	public static HashMap<String, Integer> attributeReviewIndex;
	public static ArrayList<ArrayList<String>> countDistinctFieldsArray = new ArrayList<ArrayList<String>>();
	public static int iterator=0;

	public static void test() {
		System.out.println("HEEEEIE");
	}
	
	public static void addRowtoArray(String[] list) {
		
		if(iterator==0) {
			for (int i = 0; i < list.length; i++) {
				ArrayList<String> l = new ArrayList<String>();
				countDistinctFieldsArray.add(l);
			}	
		}
		else {
			for (int i = 0; i < list.length; i++) {
				if (i>=countDistinctFieldsArray.size()) {
					countDistinctFieldsArray.add(new ArrayList<String>());
					countDistinctFieldsArray.get(i).add(list[i]);
				}
				else {
					if(!countDistinctFieldsArray.get(i).contains(list[i])) {
						countDistinctFieldsArray.get(i).add(list[i]);											
					}	
				}
			}	
		}
		System.out.println(iterator);
		iterator++;
	}
	
	
	//Parsing the price to a double
	public static Double priceAsDouble(String s){
		String price = "";
		for (int i = 0; i < s.length(); i++) {
			if (s.charAt(i) != ',' && s.charAt(i) != '$'){
				price += s.charAt(i);
			}
		}
		if (s.isEmpty()){
			return 0.0;
		}
		return Double.parseDouble(price);
	}
	
	public static String numberOfBedsToRoomType(String numberOfBeds){
		if (numberOfBeds.isEmpty()){
			return "Room for 0 persons";
		}
		int number = (int) Double.parseDouble(numberOfBeds);
		if (number == 1){
			return "Room for 1 person";
		}
		return "Room for " + number + " persons";
	}

	
	//The indexes for each attribute
	public static void mapAttributeAndIndex(JavaRDD<String> input, char csvFile){
		HashMap<String, Integer> attributeIndex = new HashMap<String, Integer>();
		String[] attributeList = input.first().split("\t");
		int index = 0;
		for (String attribute : attributeList) {
			attributeIndex.put(attribute, index);
			index++;
		}
		if (csvFile == 'l'){
			attributeListingIndex = attributeIndex;
		}
		else if (csvFile == 'c'){
			attributeCalendarIndex = attributeIndex;
		}
		else if (csvFile == 'r'){
			attributeReviewIndex = attributeIndex;
		}
	}

	//Returns a JavaRDD<String> with the the columns equal to the attributes specified in "columns"
	public static JavaRDD<String[]> mapToColumns(JavaRDD<String> inputRDD, final String[] columns, final char csvFile){
		JavaRDD<String[]> result = inputRDD.map(new Function<String, String[]>() {
			public String[] call(String s) {
				String[] entireRow = s.split("\t");
				String[] attriButesToReturn = new String[columns.length];
				for (int i = 0; i < columns.length; i++) {
					int index = -1;
					if (csvFile == 'l'){
						index = attributeListingIndex.get(columns[i]);
					}
					else if (csvFile == 'c'){
						index = attributeCalendarIndex.get(columns[i]);
					}
					attriButesToReturn[i] = entireRow[index];
				}
//				System.out.println(Arrays.toString(attriButesToReturn));
				return attriButesToReturn;
			} 
		});
		//Removes the header of the inpuRDD
		Function2 removeHeader= new Function2<Integer, Iterator<String[]>, Iterator<String[]>>(){
		    public Iterator<String[]> call(Integer ind, Iterator<String[]> iterator) throws Exception {
		    	if(ind==0 && iterator.hasNext()){
		            iterator.next();
		            return iterator;
		        }else
		            return iterator;
		    }
		};
		//		System.out.println(result.count());
		JavaRDD<String[]> resultAfterCuttingHead = result.mapPartitionsWithIndex(removeHeader, true);
		return resultAfterCuttingHead;
	}

	//FOR CITYLIST
	public static Function2<ArrayList<City>, String[], ArrayList<City>> addAndCountCity(){
		return new Function2<ArrayList<City>, String[], ArrayList<City>>() { 

			public ArrayList<City> call(ArrayList<City> cityList, String[] cityInfo)
					throws Exception {
				City city = new City(cityInfo[0], priceAsDouble(cityInfo[1]), cityInfo[2], priceAsDouble(cityInfo[3]), priceAsDouble(cityInfo[4]), priceAsDouble(cityInfo[5]), cityInfo[6], priceAsDouble(cityInfo[7]));
				if (cityExistsInCityList(cityList, city)){
					updateCity(cityList, city);
				}
				else{
					addNewCity(cityList, city);
				}
				return cityList;
			} 
		};
	}

	public static Function2<ArrayList<City>, ArrayList<City>, ArrayList<City>> combineCity(){
		return new Function2<ArrayList<City>, ArrayList<City>, ArrayList<City>>() {
			public ArrayList<City> call(ArrayList<City> cityList1, ArrayList<City> cityList2)
					throws Exception {
				for (City city2 : cityList2) {
					if (cityExistsInCityList(cityList1, city2)){
						updateCity(cityList1, city2);
					}
					else{
						addNewCity(cityList1, city2);
					}
				}
				return cityList1;
			}
		};
	}
	

	public static boolean cityExistsInCityList(ArrayList<City> cityList, City cityToCheck){
		for (City city : cityList) {
			if (city.getName().equals(cityToCheck.getName())){
				return true;
			}
		}
		return false;
	}
	
	public static void updateCity(ArrayList<City> cityList, City city){
		for (City cityToUpdate : cityList) {
			if (cityToUpdate.getName().equals(city.getName())){
				cityToUpdate.updateParameters(city);
			}
		}
	}
	
	public static void addNewCity(ArrayList<City> cityList, City city){
		cityList.add(city);
	}
	
	
	//FOR LISTINGLIST
	public static Function2<ArrayList<Listing>, String[], ArrayList<Listing>> addAndCountListing(){
		return new Function2<ArrayList<Listing>, String[], ArrayList<Listing>>() { 

			public ArrayList<Listing> call(ArrayList<Listing> listingList, String[] listingInfo)
					throws Exception {
				Listing listing = new Listing(priceAsDouble(listingInfo[0]), listingInfo[1]);
				if (listingExistsInListingList(listingList, listing)){
					updateListing(listingList, listing);
				}
				else{
					addNewListing(listingList, listing);
				}
				return listingList;
			} 
		};
	}

	public static Function2<ArrayList<Listing>, ArrayList<Listing>, ArrayList<Listing>> combineListing(){
		return new Function2<ArrayList<Listing>, ArrayList<Listing>, ArrayList<Listing>>() {
			public ArrayList<Listing> call(ArrayList<Listing> listingList1, ArrayList<Listing> listingList2)
					throws Exception {
				for (Listing listing2 : listingList2) {
					if (listingExistsInListingList(listingList1, listing2)){
						updateListing(listingList1, listing2);
					}
					else{
						addNewListing(listingList1, listing2);
					}
				}
				return listingList1;
			}
		};
	}
	
	public static boolean listingExistsInListingList(ArrayList<Listing> listingList, Listing listingToCheck){
		for (Listing listing : listingList) {
			if (listing.getId() == listingToCheck.getId()){
				return true;
			}
		}
		return false;
	}
	
	public static void updateListing(ArrayList<Listing> listingList, Listing listing){
		for (Listing listingToUpdate : listingList) {
			if (listingToUpdate.getId() == listing.getId()){
				listingToUpdate.updateAvailable(listing);
			}
		}
	}
	
	public static void addNewListing(ArrayList<Listing> listingList, Listing listing){
		listingList.add(listing);
	}
	
	public static void calculateTop3HostsWithHighestIncomeForEachCity(ArrayList<City> cityList, ArrayList<Listing> listingList){
		for (City city : cityList) {
			for (Host host : city.getHostList()) {
				for (Listing listing : listingList) {
					if (host.getListingIDs().containsKey((Integer) listing.getId())){
						host.updateTotalIncome(listing.getId(), listing.getNumberOfNotAvailable());
					}
				}
			}
			city.updateTop3Hosts();
		}
	}
	
	public static JavaPairRDD<String, String[]> mapToPair(JavaRDD<String> input, final String key, final String[] columns, final HashMap<String, Integer> attributeList){
		PairFunction<String, String, String[]> keyData = new PairFunction<String, String, String[]>() { 
			public Tuple2<String, String[]> call(String s) throws Exception {
				String[] entireRow = s.split("\t");
				String[] attriButesToReturn = new String[columns.length];
				int keyIndex = attributeList.get(key);
				for (int i = 0; i < columns.length; i++) {
					int index = attributeList.get(columns[i]);
					attriButesToReturn[i] = entireRow[index];
				}
				return new Tuple2(entireRow[keyIndex], attriButesToReturn);
			}
		};
		//Removes the header
		Function2 removeHeader= new Function2<Integer, Iterator<String[]>, Iterator<String[]>>(){
		    public Iterator<String[]> call(Integer ind, Iterator<String[]> iterator) throws Exception {
		    	if(ind==0 && iterator.hasNext()){
		            iterator.next();
		            return iterator;
		        }else
		            return iterator;
		    }
		};
		JavaPairRDD<String, String[]> listingPairs = input.mapToPair(keyData);
		listingPairs.mapPartitionsWithIndex(removeHeader, true);
		return listingPairs;
	}
	
	//Joining pair2 into pair1. pair1.leftouterJoin(Pair2).
	public static JavaPairRDD<String, String[]> lefOuterJoin(JavaPairRDD<String, String[]> pair1, JavaPairRDD<String, String[]> pair2){
		JavaPairRDD<String, Tuple2<String[], Optional<String[]>>> joinedPair = pair1.leftOuterJoin(pair2);
		
//		joinedPair.foreach(new VoidFunction<Tuple2<String, Tuple2<String[], Optional<String[]>>>>(){
//
//			public void call(
//					Tuple2<String, Tuple2<String[], Optional<String[]>>> t)
//					throws Exception {
//				System.out.println("t1: " +Arrays.toString(t._2._1));
//				System.out.println("t2: " +Arrays.toString(t._2._2.get()));
//			}
//			});
		
		
		JavaPairRDD<String, String[]> joinedAndReducedPair = joinedPair
				.mapToPair(new PairFunction<Tuple2<String, Tuple2<String[], Optional<String[]>>>, String, String[]>() {
					public Tuple2<String, String[]> call(
							Tuple2<String, Tuple2<String[], Optional<String[]>>> t)
									throws Exception {
						if (t._2._2.isPresent()){
							String[] newTuple = new String[t._2._1.length+t._2._2.get().length];
							for (int i = 0; i < t._2._1.length; i++) {
								newTuple[i] = t._2._1[i];
							}
							for (int j = t._2._1.length; j < newTuple.length; j++) {
								newTuple[j] = t._2._2.get()[j-t._2._1.length];
							}
							return new Tuple2<String, String[]>(t._1(), newTuple);
						}
						else{
							return new Tuple2<String, String[]>(t._1(), null);
						}
					}
				});
		return joinedAndReducedPair;
	}
	

}
