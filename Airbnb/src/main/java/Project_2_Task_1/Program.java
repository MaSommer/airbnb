package Project_2_Task_1;

import java.awt.geom.Point2D;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
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

import org.apache.avro.file.SyncableFileOutputStream;
import org.apache.commons.math3.util.Combinations;
import org.apache.hadoop.util.hash.Hash;
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

import airbnb.HelpMethods;
import airbnb.PolygonConstructor;
import scala.Serializable;
import scala.Tuple2;


public class Program implements Serializable {	

	private static JavaRDD<String> listings_usRDD;
	private static JavaRDD<String> reviews_usRDD;
	private static JavaRDD<String> calendar_usRDD;
	private static JavaRDD<String> neighborhood_test;
	

	public static int cityIndex;
	public static HashMap<String,Integer> mapIndex;

	private static JavaRDD<String> neighbourHoodGeosjon;


	public Program(JavaSparkContext sc) throws IOException{
		listings_usRDD = sc.textFile("target/listings_us.csv");
		reviews_usRDD = sc.textFile("target/reviews_us.csv");
		calendar_usRDD = sc.textFile("target/calendar_us.csv");
		neighborhood_test = sc.textFile("target/neighborhood_test.csv");
		neighbourHoodGeosjon = sc.textFile("target/neighbourhoods.geojson");
	}


	
	public static void tf_idf(final String input,final String nameOrID) throws IOException{
		
		if(input.equals("-n")) {
				tf_idf_n(nameOrID); 
		}
		else {
			tf_idf_l(nameOrID);
		}
	}
	
	
	private static void tf_idf_l(final String id) throws IOException {
	//Initialize the JavaRDD from listings_us.csv
			HelpMethods.mapAttributeAndIndex(listings_usRDD, 'l');
			String[] headerList = listings_usRDD.first().split("\t");
			String[] columnsNeeded = {"name","id","description"};
			
			
			JavaRDD<String> thisRDD = listings_usRDD.sample(false, 1);
			final int numDocuments = (int) thisRDD.count();
			
			//Get the JavaRDD<String[]> for the columnsNeeded
			JavaRDD<String[]> stringRDD = HelpMethods.mapToColumns(thisRDD,columnsNeeded, 'l');
			
			JavaRDD<HashMap<String,Integer>> toCountNumRDD = stringRDD.map(new Function<String[],HashMap<String,Integer>>() {

				public HashMap<String,Integer> call(String[] v1) throws Exception {
					String[] descriptionSplit = v1[2].split(" ");
					
					for (int i = 0; i < descriptionSplit.length; i++) {
						descriptionSplit[i] = descriptionSplit[i].replace(".", "");
						descriptionSplit[i] = descriptionSplit[i].replace(",", "");
						descriptionSplit[i] = descriptionSplit[i].replace("(", "");
						descriptionSplit[i] = descriptionSplit[i].replace(")", "");
						descriptionSplit[i] = descriptionSplit[i].replace("+ ", "");
						descriptionSplit[i] = descriptionSplit[i].replace("+)", "");
						descriptionSplit[i] = descriptionSplit[i].replace("!", "");
						descriptionSplit[i] = descriptionSplit[i].replace("- ", "");
						descriptionSplit[i] = descriptionSplit[i].replace("*", "");
						descriptionSplit[i] = descriptionSplit[i].trim();
						descriptionSplit[i] = descriptionSplit[i].toLowerCase();
					}
					
					HashMap<String,Integer> result = new HashMap<String, Integer>();
					for (String s : descriptionSplit) {
						result.put(s, 1);
					}
					
					return result;
					
				}
			});
			
			
			//GET HOW MANY DOCS EACH WORD IS LISTED IN
			HashMap<String,Integer> resultCountRDD = toCountNumRDD.reduce(new Function2<HashMap<String,Integer>, HashMap<String,Integer>, HashMap<String,Integer>>() {
				
				public HashMap<String, Integer> call(HashMap<String, Integer> v1, HashMap<String, Integer> v2) throws Exception {
					
					for(String s : v2.keySet()) {
						if(v1.containsKey(s)) {
							int prev = v1.get(s);
							v1.put(s, prev + 1);
						}
						else {
							v1.put(s, 1);
						}
					}
					return v1;
					
				}
			});
			
			mapIndex = resultCountRDD;
			
			//Input - Key - Value
			JavaPairRDD<String,String[]> pairRDD = stringRDD.mapToPair(new PairFunction<String[], String, String[]>() {

				public Tuple2<String, String[]> call(String[] t) throws Exception {
					String name = t[0]; 
					String id = t[1]; 
					String description = t[2]; 
					
					//Create key
					String key = id;
					
					//Split description by space
					String[] descriptionSplit = description.split(" ");
					
					//Remove comma, full stop etc
					for (int i = 0; i < descriptionSplit.length; i++) {
						descriptionSplit[i] = descriptionSplit[i].replace(".", "");
						descriptionSplit[i] = descriptionSplit[i].replace(",", "");
						descriptionSplit[i] = descriptionSplit[i].replace("(", "");
						descriptionSplit[i] = descriptionSplit[i].replace(")", "");
						descriptionSplit[i] = descriptionSplit[i].replace("+ ", "");
						descriptionSplit[i] = descriptionSplit[i].replace("+)", "");
						descriptionSplit[i] = descriptionSplit[i].replace("!", "");
						descriptionSplit[i] = descriptionSplit[i].replace("- ", "");
						descriptionSplit[i] = descriptionSplit[i].replace("*", "");
						descriptionSplit[i] = descriptionSplit[i].replace("", "");
						descriptionSplit[i] = descriptionSplit[i].trim();
						descriptionSplit[i] = descriptionSplit[i].toLowerCase();
					}
					
					return new Tuple2<String, String[]>(key, descriptionSplit);
				}
			});
			
			
			JavaPairRDD<String, HashMap<String, Integer>> resultMapping = pairRDD.mapToPair(new PairFunction<Tuple2<String,String[]>, String, HashMap<String,Integer>>() {

				public Tuple2<String, HashMap<String, Integer>> call(Tuple2<String, String[]> t) throws Exception {
						
					HashMap<String, Integer> mapWordCount = new HashMap<String, Integer>();
				
					//Count the number of times each word appears
					for (String word : t._2) {
						if(mapWordCount.containsKey(word)) {
							int previousCount = mapWordCount.get(word);
							mapWordCount.put(word, previousCount+1);
						}
						else {
							mapWordCount.put(word,1);
						}
					}
				
					return new Tuple2<String, HashMap<String,Integer>>(t._1, mapWordCount);
				}
			});
			
			
			//Create sorted ArrayList<Word>
			JavaPairRDD<String, ArrayList<Word>> resultMappingToAL = resultMapping.mapToPair(new PairFunction<Tuple2<String,HashMap<String,Integer>>, String, ArrayList<Word>>() {

				public Tuple2<String, ArrayList<Word>> call(Tuple2<String, HashMap<String, Integer>> t) throws Exception {
					
					ArrayList<Word> result = new ArrayList<Word>();
					
					int numWordsInDesctription = 0;
					for (String word : t._2.keySet()) {
						numWordsInDesctription += t._2.get(word);
					}
					
					
					for (String word : t._2.keySet()) {
						Word w = new Word(word,t._2.get(word));
						w.tf = (double)w.count/(double)numWordsInDesctription;
						w.idf = (double)numDocuments/(double)mapIndex.get(word);
						w.weight = w.tf*w.idf;
						result.add(w);
							
					}
					
					Collections.sort(result, new Comparator<Word>() {

						public int compare(Word o1, Word o2) {
							return o1.compareTo(o2);
						}
					});
					
					ArrayList<Word> resultReturn = new ArrayList<Word>();
					for (Word w : result) {
						if(resultReturn.size()<100) {
							resultReturn.add(w);						
						}
						else {
							break;
						}
					}
					
					return new Tuple2<String, ArrayList<Word>>(t._1, resultReturn);
				}
				
			});
			
			
			JavaPairRDD<String,ArrayList<Word>> hei = resultMappingToAL.filter(new Function<Tuple2<String,ArrayList<Word>>, Boolean>() {
				
				public Boolean call(Tuple2<String, ArrayList<Word>> v1) throws Exception {
					if(v1._1.equals(id)) {
						return true;
					}
					else {
						return false;
					}
				}
			});
			
			JavaRDD<ArrayList<Word>> r = hei.map(new Function<Tuple2<String,ArrayList<Word>>, ArrayList<Word>>() {

				public ArrayList<Word> call(Tuple2<String, ArrayList<Word>> v1) throws Exception {
					return v1._2;
				}
			});
			
			r.foreach(new VoidFunction<ArrayList<Word>>() {
				
				public void call(ArrayList<Word> t) throws Exception {
					for (Word w : t) {
						System.out.println(w.word + " " + w.weight);
					}
					
				}
			});
			
			JavaRDD<ArrayList<String>> res = r.map(new Function<ArrayList<Word>, ArrayList<String>>() {

				public ArrayList<String> call(ArrayList<Word> v1) throws Exception {
					
					ArrayList<String> result = new ArrayList<String>();
					
					for(Word w : v1) {
						result.add(w.word+" has weight: "+w.weight+"\n");
					}
					
					return result;
					 
				}
			});
			
			res.coalesce(1).saveAsTextFile("target/pus12");
	}
	
	
	
	
	
	
	
	//////////////////////////////////////////////////////////////////////////////////////////////////////////
	
	
	
	///Method for neighborhood

	private static void tf_idf_n(String neighborhood) throws IOException {
		
		HelpMethods.mapAttributeAndIndex(listings_usRDD, 'l');
		
		String[] columns_listings = {"id","description","latitude","longitude"};
		JavaRDD<String[]> listingsRDD = HelpMethods.mapToColumns(listings_usRDD, columns_listings,'l');
		
		
		//Get the requested neigborhood as a Polygonconstructor
		ArrayList<PolygonConstructor> listWithPolygons = HelpMethods.createPolygons();
		
		PolygonConstructor thisNeighborhood = new PolygonConstructor(new ArrayList<Double>(), new ArrayList<Double>(), "", "");

		for (PolygonConstructor neigh : listWithPolygons) {
			if(neigh.getNeighbourhood().equals(neighborhood)) {
				thisNeighborhood = neigh;
			}
		}
		
		
		final PolygonConstructor res = thisNeighborhood;
		
		JavaPairRDD<String, String[]> resultFromMapToPair = listingsRDD.mapToPair(new PairFunction<String[], String, String[]>() {

			public Tuple2<String, String[]> call(String[] t) throws Exception {
				String id = t[0];
				String description = t[1];
				String longitude = t[3];
				String latitude = t[2];
				String key = "";

				
				if(res.checkIfInsideOfPath(new Point2D.Double((HelpMethods.stringToDouble(longitude)),HelpMethods.stringToDouble(latitude)))) {
					key = "Yes";
				}
				else {
					key = id;
				}
				
				//Split description by space
				String[] descriptionSplit = description.split(" ");
				
				//Remove comma, full stop etc
				for (int i = 0; i < descriptionSplit.length; i++) {
					descriptionSplit[i] = descriptionSplit[i].replace(".", "");
					descriptionSplit[i] = descriptionSplit[i].replace(",", "");
					descriptionSplit[i] = descriptionSplit[i].replace("(", "");
					descriptionSplit[i] = descriptionSplit[i].replace(")", "");
					descriptionSplit[i] = descriptionSplit[i].replace("+ ", "");
					descriptionSplit[i] = descriptionSplit[i].replace("+)", "");
					descriptionSplit[i] = descriptionSplit[i].replace("!", "");
					descriptionSplit[i] = descriptionSplit[i].replace("- ", "");
					descriptionSplit[i] = descriptionSplit[i].replace("*", "");
					descriptionSplit[i] = descriptionSplit[i].trim();
					descriptionSplit[i] = descriptionSplit[i].toLowerCase();
				}
				
				return new Tuple2<String, String[]>(key, descriptionSplit);
				
			}
		});
		
		
		//All descriptions of provided neighborhood is in one row, as a String[]
		JavaPairRDD<String,String[]> resultFromReducingByKey = resultFromMapToPair.reduceByKey(new Function2<String[], String[], String[]>() {
			
			public String[] call(String[] v1, String[] v2) throws Exception {
				String[] result = new String[v1.length+v2.length];
				for (int i = 0; i < v1.length; i++) {
					result[i] = v1[i];
				}
				for (int i = 0; i < v2.length; i++) {
					result[i] = v2[i];
				}
				return result;
			}
		});
		
		final int numDocuments = (int) resultFromReducingByKey.count();
		
		//1) Get how many times each word is listed in this document
		JavaPairRDD<String, HashMap<String, Integer>> mapToHashMap = resultFromReducingByKey.mapToPair(new PairFunction<Tuple2<String,String[]>, String, HashMap<String,Integer>>() {

			public Tuple2<String, HashMap<String, Integer>> call(Tuple2<String, String[]> t) throws Exception {
				
				HashMap<String, Integer> mapWordCount = new HashMap<String, Integer>();
				
				//Count the number of times each word appears
				for (String word : t._2) {
					if(mapWordCount.containsKey(word)) {
						int previousCount = mapWordCount.get(word);
						mapWordCount.put(word, previousCount+1);
					}
					else {
						mapWordCount.put(word,1);
					}
				}
			
				return new Tuple2<String, HashMap<String,Integer>>(t._1, mapWordCount);
			}
		});
		

		
		//2) Get how many documents this word is in
		JavaRDD<HashMap<String,Integer>> toCountNumRDD = resultFromReducingByKey.map(new Function<Tuple2<String,String[]>, HashMap<String,Integer>>() {

			public HashMap<String,Integer> call(Tuple2<String, String[]> v1) throws Exception {
				HashMap<String,Integer> resultMap = new HashMap<String, Integer>();
				
				for (String word : v1._2) {
					resultMap.put(word, 1);
				}
				
				return resultMap;
			}
		});
		
		HashMap<String,Integer> resultCountRDD = toCountNumRDD.reduce(new Function2<HashMap<String,Integer>, HashMap<String,Integer>, HashMap<String,Integer>>() {
			
			public HashMap<String, Integer> call(HashMap<String, Integer> v1, HashMap<String, Integer> v2) throws Exception {
				
				for(String s : v2.keySet()) {
					if(v1.containsKey(s)) {
						int prev = v1.get(s);
						v1.put(s, prev + 1);
					}
					else {
						v1.put(s, 1);
					}
				}
				return v1;
				
			}
		});
		
		mapIndex = resultCountRDD;
		
		
		
		//3) Now - create sorted ArrayList<Word>
		JavaPairRDD<String, ArrayList<Word>> resultMappingToAL = mapToHashMap.mapToPair(new PairFunction<Tuple2<String,HashMap<String,Integer>>, String, ArrayList<Word>>() {

			public Tuple2<String, ArrayList<Word>> call(Tuple2<String, HashMap<String, Integer>> t) throws Exception {
				
				ArrayList<Word> result = new ArrayList<Word>();
				
				int numWordsInDesctription = 0;
				for (String word : t._2.keySet()) {
					numWordsInDesctription += t._2.get(word);
				}
				
				
				for (String word : t._2.keySet()) {
					Word w = new Word(word,t._2.get(word));
					w.tf = (double)w.count/(double)numWordsInDesctription;
					w.idf = (double)numDocuments/(double)mapIndex.get(word);
					w.weight = w.tf*w.idf;
					result.add(w);
						
				}
				
				Collections.sort(result, new Comparator<Word>() {

					public int compare(Word o1, Word o2) {
						return o1.compareTo(o2);
					}
				});
				
				ArrayList<Word> resultReturn = new ArrayList<Word>();
				for (Word w : result) {
					if(resultReturn.size()<100) {
						resultReturn.add(w);						
					}
					else {
						break;
					}
				}
				
				return new Tuple2<String, ArrayList<Word>>(t._1, resultReturn);
			}
			
		});
		
		JavaPairRDD<String, ArrayList<Word>> finalResult = resultMappingToAL.filter(new Function<Tuple2<String,ArrayList<Word>>, Boolean>() {
			
			public Boolean call(Tuple2<String, ArrayList<Word>> v1) throws Exception {
				if(v1._1.equals("Yes")){
					return true;
				}
				return false;
			}
		});
		
		finalResult.foreach(new VoidFunction<Tuple2<String,ArrayList<Word>>>() {
			
			public void call(Tuple2<String, ArrayList<Word>> t) throws Exception {
				int count=0;
				for(Word w : t._2) {
					System.out.println(count + " " +w.word + " " + w.weight);		
					count++;
				}
				
			}
		});	
		
		JavaRDD<ArrayList<String>> r = finalResult.map(new Function<Tuple2<String,ArrayList<Word>>, ArrayList<String>>() {

			public ArrayList<String> call(Tuple2<String, ArrayList<Word>> v1) throws Exception {
					ArrayList<String> result = new ArrayList<String>();
				
					for(Word w : v1._2) {
						result.add(w.word+" has weight: "+w.weight+"\n");
					}
					
					return result;
			}
		});
		
		
		r.coalesce(1).saveAsTextFile("/target/heisannnnnn");
	}

	public static void main(String[] args) throws IOException {
		SparkConf conf = new SparkConf()
		.setAppName("AirbnbData")
		.setMaster("local[*]")
		;
		JavaSparkContext sc = new JavaSparkContext(conf);

		Program p=new Program(sc);
		String input = "-l";
		String nameOrID = "4717459";
		
		p.tf_idf(input, nameOrID);
	}
}
