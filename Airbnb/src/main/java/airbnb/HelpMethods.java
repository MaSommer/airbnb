package airbnb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

public class HelpMethods {

	public static HashMap<String, Integer> attributeIndex;

	
	//Parsing the price to a double
	public static Double priceAsDouble(String s){
		String price = "";
		for (int i = 0; i < s.length(); i++) {
			if (s.charAt(i) != ',' && s.charAt(i) != '$'){
				price += s.charAt(i);
			}
		}
		return Double.parseDouble(price);
	}

	
	//The indexes for each attribute
	public static void mapAttributeAndIndex(JavaRDD<String> input){
		attributeIndex = new HashMap<String, Integer>();
		String[] attributeList = input.first().split("\t");
		int index = 0;
		for (String attribute : attributeList) {
			attributeIndex.put(attribute, index);
			index++;
		}
	}

	//Returns a JavaRDD<String> with the the columns equal to the attributes specified in "columns"
	public static JavaRDD<String[]> mapToColumns(JavaRDD<String> inputRDD, final String[] columns){
		JavaRDD<String[]> result = inputRDD.map(new Function<String, String[]>() {
			public String[] call(String s) {
				String[] entireRow = s.split("\t");
				String[] attriButesToReturn = new String[columns.length];
				for (int i = 0; i < columns.length; i++) {
					int index = attributeIndex.get(columns[i]);
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

	public static Function2<ArrayList<City>, String[], ArrayList<City>> addAndCount(){
		return new Function2<ArrayList<City>, String[], ArrayList<City>>() { 

			public ArrayList<City> call(ArrayList<City> cityList, String[] cityInfo)
					throws Exception {
				City city = new City(cityInfo[0], priceAsDouble(cityInfo[1]));
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

	public static Function2<ArrayList<City>, ArrayList<City>, ArrayList<City>> combine(){
		return new Function2<ArrayList<City>, ArrayList<City>, ArrayList<City>>() {
			public City call(City city1, City city2) throws Exception {
				if (city1.getName().equals(city2.getName())){
					city1.updateParameters(city2);
					System.out.println("City1: " + city1.getNumberOfRooms());
					System.out.println("City2: " + city2.getNumberOfRooms());
				}
				return city1;
			}

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
	
	

}
