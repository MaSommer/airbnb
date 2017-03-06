package airbnb;

import scala.Serializable;

public class RoomType implements Serializable{
	
	private final String roomType;
	private double avgPricePerNight;

	private double totalPricePerNight;
	private double numberOfThisRoomType;
	

	public RoomType(String roomtype, double price){
		this.roomType = roomtype;
		this.numberOfThisRoomType = 1;
		this.totalPricePerNight = price;
		updateAveragePricePerNight();
	}
	
	private void updateAveragePricePerNight(){
		this.avgPricePerNight = this.totalPricePerNight/this.numberOfThisRoomType;
	}
	
	public void updateAvgRoomTypePrice(RoomType roomType){
		this.numberOfThisRoomType += roomType.getNumberOfThisRoomType();
		this.totalPricePerNight += roomType.getAvgPricePerNight();
		updateAveragePricePerNight();
	}

	public String getRoomType() {
		return roomType;
	}
	
	public double getAvgPricePerNight() {
		return avgPricePerNight;
	}
	
	public double getNumberOfThisRoomType() {
		return numberOfThisRoomType;
	}
}
