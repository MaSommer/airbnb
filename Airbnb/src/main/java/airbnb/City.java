package airbnb;

import scala.Serializable;

public class City implements Serializable{
	
	private final String name;
	private int numberOfRooms;

	private double totalPricePerNight;
	private double avgPricePerNight;
	
	public City(String name, Double price){
		this.name = name.trim();
		this.numberOfRooms = 1;
		this.totalPricePerNight = price;
	}
	
	//Updates the values for the city
	public void updateParameters(City city){
		this.totalPricePerNight += city.totalPricePerNight;
		this.numberOfRooms += city.numberOfRooms;
		updateAveragePricePerNight();
	}
	
	private void updateAveragePricePerNight(){
		avgPricePerNight = totalPricePerNight/numberOfRooms;
	}
	
	public double getAveragePricePerNight(){
		return avgPricePerNight;
	}
	
	
	public String getName(){
		return name;
	}
	
	public int getNumberOfRooms() {
		return numberOfRooms;
	}
	
	public double getTotalPricePerNight() {
		return totalPricePerNight;
	}

}
