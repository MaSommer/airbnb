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

	//Returns a JavaRDD<String> with the the columns equal to the attributes specified in "columns"
	public static JavaRDD<String> mapToColumnsString(JavaRDD<String> inputRDD, final String column, final char csvFile){
		JavaRDD<String> result = inputRDD.map(new Function<String, String>() {
			public String call(String s) {
				String[] entireRow = s.split("\t");
				String attriButeToReturn;
				attriButeToReturn = entireRow[attributeListingIndex.get(column)];
	
				return attriButeToReturn;
			} 
		});
		return result;
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
