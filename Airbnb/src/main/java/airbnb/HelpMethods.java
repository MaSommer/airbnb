package airbnb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
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


	//Parsing the price to a double
	public static Double stringToDouble(String s){
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

	public static JavaPairRDD<String, String[]> mapToPair(JavaRDD<String> input, final String[] key, final String[] columns, final HashMap<String, Integer> attributeList){

		PairFunction<String, String, String[]> keyData = new PairFunction<String, String, String[]>() { 

			public Tuple2<String, String[]> call(String s) throws Exception {
				String[] entireRow = s.split("\t");
				String[] attriButesToReturn = new String[columns.length];
				String keyToReturn = "";
				for (int i = 0; i < key.length; i++) {
					keyToReturn += entireRow[attributeList.get(key[i])];
					if (i != key.length-1){
						keyToReturn += " ";
					}
				}
				for (int i = 0; i < columns.length; i++) {
					int index = attributeList.get(columns[i]);
					attriButesToReturn[i] = entireRow[index];
				}
				return new Tuple2(keyToReturn, attriButesToReturn);
			}
		};
		JavaPairRDD<String, String[]> listingPairs = input.mapToPair(keyData);

		JavaPairRDD<String, String[]> listingPairsWithoutHeader = listingPairs.filter(new Function<Tuple2<String, String[]>, Boolean>(){
			public Boolean call(Tuple2<String, String[]> v1) throws Exception {
				for (int i = 0; i < columns.length; i++) {
					if (!columns[i].equals(v1._2[i])){
						return true;
					}
				}
				return false;
			}

		});
		return listingPairsWithoutHeader;
	}
	
	public static JavaPairRDD<String, String[]> mapToPairNewKey(JavaPairRDD<String, String[]> input, final int[] keyIndexes){
		PairFunction<Tuple2<String, String[]>, String, String[]> keyData = new PairFunction<Tuple2<String, String[]>, String, String[]>() { 
			public Tuple2<String, String[]> call(Tuple2<String, String[]> t)
					throws Exception {
				String[] attriButesToReturn = t._2;
				
				String key = "";
				for (int i = 0; i < keyIndexes.length; i++) {
					key += attriButesToReturn[keyIndexes[i]];
					if (i < keyIndexes.length-1){
						key+= " ";
					}
				}
				return new Tuple2(key, attriButesToReturn);
			}
		};
		JavaPairRDD<String, String[]> listingPairs = input.mapToPair(keyData);
		return listingPairs;
	}
	
	public static JavaPairRDD<String, String[]> mapToPair(JavaPairRDD<String, String[]> input, final int[] keyIndexes){
		PairFunction<Tuple2<String, String[]>, String, String[]> keyData = new PairFunction<Tuple2<String, String[]>, String, String[]>() { 
			public Tuple2<String, String[]> call(Tuple2<String, String[]> t)
					throws Exception {
				String[] attriButesToReturn = t._2;
				
				String key = "";
				for (int i = 0; i < keyIndexes.length; i++) {
					key += attriButesToReturn[keyIndexes[i]];
					if (i < keyIndexes.length-1){
						key+= " ";
					}
				}
				return new Tuple2(key, attriButesToReturn);
			}
		};
		JavaPairRDD<String, String[]> listingPairs = input.mapToPair(keyData);
		return listingPairs;
	}

	//Joining pair2 into pair1. pair1.leftouterJoin(Pair2).
	public static JavaPairRDD<String, String[]> lefOuterJoin(JavaPairRDD<String, String[]> pair1, JavaPairRDD<String, String[]> pair2){
		JavaPairRDD<String, Tuple2<String[], Optional<String[]>>> joinedPair = pair1.leftOuterJoin(pair2);

		JavaPairRDD<String, String[]> joinedAndReducedPair = joinedPair
				.mapToPair(new PairFunction<Tuple2<String, Tuple2<String[], Optional<String[]>>>, String, String[]>() {
					public Tuple2<String, String[]> call(
							Tuple2<String, Tuple2<String[], Optional<String[]>>> t)
									throws Exception {
						if (t._2._2.isPresent()){
							String[] newTuple = new String[t._2._1.length+t._2._2.get().length-1];
							for (int i = 0; i < t._2._1.length; i++) {
								newTuple[i] = t._2._1[i];
							}
							for (int j = t._2._1.length; j < newTuple.length; j++) {
								//make sure to not duplicate the key in the tupple
								if (t._2._2.get()[j-t._2._1.length].equals(t._1)){
									if (t._2._2.get()[j-t._2._1.length+1].equals("t")){
										newTuple[j] = "0";																																							
									}
									else if (t._2._2.get()[j-t._2._1.length+1].equals("f")){
										newTuple[j] = "1";																													
									}
									else{
										newTuple[j] = t._2._2.get()[j-t._2._1.length+1];																			
									}
									continue;
								}
								newTuple[j] = t._2._2.get()[j-t._2._1.length];
							}
							return new Tuple2<String, String[]>(t._1, newTuple);
						}
						else{
							return new Tuple2<String, String[]>(t._1, t._2._1);
						}
					}
				});
		return joinedAndReducedPair;
	}

	public static JavaPairRDD<String, String[]> reduceByKeyOnAvailable(JavaPairRDD<String, String[]> joinedPair){


		JavaPairRDD<String, String[]> joinedPairReducedByKey = joinedPair.reduceByKey(new Function2<String[], String[], String[]>(){
			public String[] call(String[] v1, String[] v2) {
				int numberOfNotAvailable1 = Integer.parseInt(v1[v1.length-1]);
				int numberOfNotAvailable2 = Integer.parseInt(v2[v2.length-1]);
				int sum = numberOfNotAvailable1 + numberOfNotAvailable2;
				v1[v1.length-1] = ""+sum;
				//				System.out.println(Arrays.toString(v1));
				return v1;
			}
		});
		//		System.out.println(joinedPair.count());
		return joinedPairReducedByKey;
	}
	
	public static JavaPairRDD<String, String[]> reduceByKeyOnHostId(JavaPairRDD<String, String[]> joinedPair){
		JavaPairRDD<String, String[]> joinedPairReducedByKey = joinedPair.reduceByKey(new Function2<String[], String[], String[]>(){
			public String[] call(String[] v1, String[] v2) {
				return v1;
			}
		});
		//		System.out.println(joinedPair.count());
		return joinedPairReducedByKey;
	}
	
	public static JavaPairRDD<String, String[]> reduceByKeySummingNumberOfReviews(JavaPairRDD<String, String[]> joinedPair){
		JavaPairRDD<String, String[]> joinedPairReducedByKey = joinedPair.reduceByKey(new Function2<String[], String[], String[]>(){
			public String[] call(String[] v1, String[] v2) {
				double numberOfReviews1 = stringToDouble(v1[3]);
				double numberOfReviews2 = stringToDouble(v2[3]);
				v1[3] = (double)(numberOfReviews1+numberOfReviews2) + "";
				return v1;
			}
		});
		//		System.out.println(joinedPair.count());
		return joinedPairReducedByKey;
	}
	
	public static JavaPairRDD<String, String[]> reduceByKeySummingTotalAmountSpent(JavaPairRDD<String, String[]> joinedPair){
		JavaPairRDD<String, String[]> joinedPairReducedByKey = joinedPair.reduceByKey(new Function2<String[], String[], String[]>(){
			public String[] call(String[] v1, String[] v2) {
				double numberOfReviews1 = stringToDouble(v1[5]);
				double numberOfReviews2 = stringToDouble(v2[5]);
				v1[5] = (double)(numberOfReviews1+numberOfReviews2) + "";
				return v1;
			}
		});
		//		System.out.println(joinedPair.count());
		return joinedPairReducedByKey;
	}

	public static Function2<String[], String[], String[]> addAndCombinePairAverage(){

		return new  Function2<String[], String[], String[]>() {
			public String[] call(String[] v1, String[] v2) throws Exception {
				//{"key", "total", "number"}
				v1[0] = v2[0];
				if (v1[1] == null){
					v1[1] = v2[1];
				}else{
					v1[1] = (double)(stringToDouble(v1[1]) + stringToDouble(v2[1]))+"";
				}
				if (v1[2] == null){
					v1[2] = "1";
				}
				else{
					v1[2] = (int)(stringToDouble(v1[2]) + 1)+"";					
				}
				return v1;
			}
		};
	}
	
	public static Function2<String[], String[], String[]> combinePairAverage(){
		return new Function2<String[], String[], String[]>(){

			public String[] call(String[] v1, String[] v2) throws Exception {
				//{"key", "total", "number"}
				String key = v1[0];
				String total = (double)(stringToDouble(v1[1]) + stringToDouble(v2[1]))+"";
				String number = (int)(stringToDouble(v1[2]) + stringToDouble(v2[2]))+"";
				String[] ret = {key, total, number};
				return ret;
			}
		};
	}
	
	public static Function2<String[], String[], String[]> addAndCombineEstimatedNumberOfNights(){
		
		return new  Function2<String[], String[], String[]>() {
			public String[] call(String[] v1, String[] v2) throws Exception {
				//{"key", "total", "number"}
				v1[0] = v2[0];
				if (v1[1] == null){
					v1[1] = (double)(stringToDouble(v2[1])/0.7*3)+"";
				}else{
					v1[1] = (double)(stringToDouble(v1[1]) + stringToDouble(v2[1])/0.7*3)+"";
				}
				return v1;
			}
		};
	}
	public static Function2<String[], String[], String[]> combinePairEstimatedNumberOfNights(){
		return new Function2<String[], String[], String[]>(){
			
			public String[] call(String[] v1, String[] v2) throws Exception {
				//{"key", "total", "number"}
				String key = v1[0];
				String total = (double)(stringToDouble(v1[1]) + stringToDouble(v2[1]))+"";
				String[] ret = {key, total};
				return ret;
			}
		};
	}
	
	public static Function2<String[], String[], String[]> addAndCombineAmountOfMoneySpent(){
		
		return new  Function2<String[], String[], String[]>() {
			public String[] call(String[] v1, String[] v2) throws Exception {
				//{"key", "total amount of money spent"}
				v1[0] = v2[0];
				if (v1[1] == null){
					v1[1] = (double)(stringToDouble(v2[1])/0.7*3*12*stringToDouble(v2[2]))+"";
				}else{
					v1[1] = (double)(stringToDouble(v1[1]) + stringToDouble(v2[1])/0.7*3*12*stringToDouble(v2[2]))+"";
				}
				String[] ret = {v1[0], v1[1]};
				return ret;
			}
		};
	}
	public static Function2<String[], String[], String[]> combinePairAmountOfMoneySpent(){
		return new Function2<String[], String[], String[]>(){
			
			public String[] call(String[] v1, String[] v2) throws Exception {
				//{"key", "total", "number"}
				String key = v1[0];
				String total = (double)(stringToDouble(v1[1]) + stringToDouble(v2[1]))+"";
				String[] ret = {key, total};
				return ret;
			}
		};
	}
	
	public static Function2<ArrayList<Host>, String[], ArrayList<Host>> addAndCombineTop3Hosts(){
		
		return new  Function2<ArrayList<Host>, String[], ArrayList<Host>>() {

			public ArrayList<Host> call(ArrayList<Host> hostList, String[] v2)
					throws Exception {
				//{"city", "price", "id", "host_id", "host_name", "host_total_listings_count"}
				double id = stringToDouble(v2[3]);
				String hostName = v2[4];
				double totalListings = stringToDouble(v2[5]);
				double price = stringToDouble(v2[1]);
				double numberOfNightsNotAvailable = stringToDouble(v2[6]);
				Host host = new Host((int) id, hostName, (int)totalListings, price, (int) numberOfNightsNotAvailable);
				hostList = updateTop3Hosts(hostList, host);
				return hostList;
			}
		};
	}
	public static ArrayList<Host> updateTop3Hosts(ArrayList<Host> hostList, Host host){
		if (hostList.contains(host)){
			hostList.remove(host);
		}
		//adds host if null in host list
		if (hostList.size() < 3){
			hostList.add(host);
		}
		else{
			for (int i = 0; i < 3; i++) {
				if (host.getTotalIncome() > hostList.get(i).getTotalIncome()){
					hostList.set(i, host);
					hostList.remove(hostList.size()-1);
					break;
				}
			}
		}
		//sort the hosts from high to low total income
		Collections.sort(hostList, new Comparator<Host>() {
			public int compare(Host h1, Host h2) {
				if (h1.getTotalIncome() < h2.getTotalIncome()){
					return 1;
				}
				else if (h1.getTotalIncome() > h2.getTotalIncome()){
					return -1;
				}
				else{
					return 0;
				}
			}
		});
		return hostList;
	}
	
	public static Function2<ArrayList<Host>, ArrayList<Host>, ArrayList<Host>> combinePairTop3Hosts(){
		return new Function2<ArrayList<Host>, ArrayList<Host>, ArrayList<Host>>(){

			public ArrayList<Host> call(ArrayList<Host> hostList1, ArrayList<Host> hostList2)
					throws Exception {
				for (Host host2 : hostList2) {
					for (Host host1 : hostList1) {
						if (host1.getId() == host2.getId()){
							host2.updateTotalIncome(host1);
						}
					}
					hostList1 = updateTop3Hosts(hostList1, host2);
				}
				return hostList1;
			}
		};
	}
	
	public static JavaPairRDD<String, String[]> mapToPairAddNumberOfReviewers(JavaPairRDD<String, String[]> input){
		return input.mapToPair(new PairFunction<Tuple2<String, String[]>, String, String[]>(){
			public Tuple2<String, String[]> call(Tuple2<String, String[]> t)
					throws Exception {
				String[] ret = new String[t._2.length+1];
				for (int i = 0; i < ret.length; i++) {
					if (i == ret.length-1){
						ret[i] = "1";
					}
					else{
						ret[i] = t._2[i];
					}
				}
				return new Tuple2<String, String[]>(t._1, ret);
			}
			
		});
	}
	
	public static JavaPairRDD<String, String[]> mapToPairAddTotalAmountSpent(JavaPairRDD<String, String[]> input){
		return input.mapToPair(new PairFunction<Tuple2<String, String[]>, String, String[]>(){
			public Tuple2<String, String[]> call(Tuple2<String, String[]> t)
					throws Exception {
				String[] ret = new String[t._2.length+1];
				for (int i = 0; i < ret.length; i++) {
					if (i == ret.length-1){
						double spent = 3*stringToDouble(t._2[4]);
						ret[i] = spent+"";
					}
					else{
						ret[i] = t._2[i];
					}
				}
				return new Tuple2<String, String[]>(t._1, ret);
			}
			
		});
	}
	
	public static Function2<ArrayList<Reviewer>, String[], ArrayList<Reviewer>> addAndCombineTop3Reviewers(){
		
		return new  Function2<ArrayList<Reviewer>, String[], ArrayList<Reviewer>>() {
			
			public ArrayList<Reviewer> call(ArrayList<Reviewer> reviewerList, String[] v2)
					throws Exception {
				//{listing_id, review_id, reviewer_name, number_of_reviewers_for_this_reviewer, city, price}
				double reviewer_id = stringToDouble(v2[1]);
				String reviewer_name = v2[2];
				double price = stringToDouble(v2[5]);
				double number_of_reviews_for_this_city = stringToDouble(v2[3]);
				
				Reviewer reviewer = new Reviewer(reviewer_name, (int) reviewer_id, price, (int) number_of_reviews_for_this_city);
				reviewerList = updateTop3Reviewers(reviewerList, reviewer);

				return reviewerList;
			}
		};
	}
	public static ArrayList<Reviewer> updateTop3Reviewers(ArrayList<Reviewer> reviewerList, Reviewer reviewer){
		if (reviewerList.contains(reviewer)){
			reviewerList.remove(reviewer);
		}
		//adds host if null in host list
		if (reviewerList.size() < 3){
			reviewerList.add(reviewer);
		}
		else{
			for (int i = 0; i < 3; i++) {
				if (reviewer.getNumberOfReviews() > reviewerList.get(i).getNumberOfReviews()){
					reviewerList.set(i, reviewer);
					reviewerList.remove(reviewerList.size()-1);
					break;
				}
			}
		}
		//sort the hosts from high to low total income
		Collections.sort(reviewerList, new Comparator<Reviewer>() {
			public int compare(Reviewer r1, Reviewer r2) {
				if (r1.getNumberOfReviews() < r2.getNumberOfReviews()){
					return 1;
				}
				else if (r1.getNumberOfReviews() > r2.getNumberOfReviews()){
					return -1;
				}
				else{
					return 0;
				}
			}
		});
		return reviewerList;
	}
	
	public static Function2<ArrayList<Reviewer>, ArrayList<Reviewer>, ArrayList<Reviewer>> combinePairTop3Reviewers(){
		return new Function2<ArrayList<Reviewer>, ArrayList<Reviewer>, ArrayList<Reviewer>>(){
			
			public ArrayList<Reviewer> call(ArrayList<Reviewer> reviewerList1, ArrayList<Reviewer> reviewerList2)
					throws Exception {
				for (Reviewer reviewer2 : reviewerList2) {
					for (Reviewer reviewer1 : reviewerList1) {
						if (reviewer1.getId() == reviewer2.getId()){
							reviewer2.updateParameters(reviewer1);;
						}
					}
					reviewerList1 = updateTop3Reviewers(reviewerList1, reviewer2);
				}
				return reviewerList1;
			}
		};
	}
	
	public static Function2<double[], Tuple2<String, String[]>, double[]> addAndCombineAverageNumberOfListings(){
		
		return new  Function2<double[], Tuple2<String, String[]>, double[]>() {

			public double[] call(double[] v1, Tuple2<String, String[]> v2)
					throws Exception {
				v1[0] += stringToDouble(v2._2[1]);
				v1[1] ++;
				if (stringToDouble(v2._2[1]) > 1){
					v1[2]++;
				}
				return v1;
			}
			
		};
	}
	public static Function2<double[], double[], double[]> combinePairAverageNumberOfListings(){
		return new Function2<double[], double[], double[]>(){

			public double[] call(double[] v1, double[] v2) throws Exception {
				v1[0] += v2[0];
				v1[1] += v2[1];
				v1[2] += v2[2];
				return v1;
			}
		};
	}
	
	public static Function2<String[], Tuple2<String, String[]>, String[]> addAndCountTotalAmountSpent(){
		
		return new  Function2<String[], Tuple2<String, String[]>, String[]>() {
			
			public String[] call(String[] v1, Tuple2<String, String[]> t2)
					throws Exception {
				double r1 = stringToDouble(v1[v1.length-1]);
				double r2 = stringToDouble(t2._2[t2._2.length-1]);
				if (r1 > r2){
					return v1;
				}
				else{
					return t2._2;
				}
			}
		};
	}
	public static Function2<String[], String[], String[]> combinePairTotalAmountSpent(){
		return new Function2<String[], String[], String[]>(){
			
			public String[] call(String[] v1, String[] v2) throws Exception {
				if (stringToDouble(v1[v1.length-1]) > stringToDouble(v2[v2.length-1])){
					return v1;
				}
				else{
					return v2;
				}
			}
		};
	}
	
}
