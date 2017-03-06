package airbnb;

import java.util.ArrayList;
import java.util.HashMap;

import scala.Serializable;

public class City implements Serializable{
	
	private final String name;
	private int numberOfRooms;

	private double totalPricePerNight;
	private double avgPricePerNight;
	
	private int totalReviewsPerMonth;
	private double avgNumberOfReviewsPerMonth;
	private double estimatedNumberOfNightsBookedPerYear;
	private double estimatedAmountOfMoneySpentPerYear;

	private ArrayList<RoomType> roomTypeList;
	private ArrayList<Host> hostList;
	
	private Host[] top3Hosts;

	private final double INF = Double.MAX_VALUE;

	//constructor for calculating average price per room type
	public City(String name, Double price, String roomtype, double reviewsPerMonth, double listingID, double hostID, String hostName, double totalListingsForHost){
		roomTypeList = new ArrayList<RoomType>();
		this.name = name;
		this.numberOfRooms = 1;
		this.totalPricePerNight = price;
		this.totalReviewsPerMonth = (int)reviewsPerMonth;
		updateAveragePricePerNight();
		updateAverageReviewsPerMonth();
		updateEstimatedNumberOfNightsBookedPerYear();
		updateEstimatedAmountOfMoneySpentPerYear(reviewsPerMonth, price);
		updateAvgPricePerRoomType(new RoomType(roomtype, price));
		
		hostList = new ArrayList<Host>();
		Host host = new Host((int) hostID, hostName, (int) listingID, (int) totalListingsForHost, price);
		hostList.add(host);
	}
	
	//Updates the values for the city
	public void updateParameters(City city){
		this.totalPricePerNight += city.totalPricePerNight;
		this.numberOfRooms += city.numberOfRooms;
		this.totalReviewsPerMonth += city.totalReviewsPerMonth;
		this.estimatedAmountOfMoneySpentPerYear += city.estimatedAmountOfMoneySpentPerYear;
		updateAveragePricePerNight();
		updateAverageReviewsPerMonth();
		updateEstimatedNumberOfNightsBookedPerYear();
		//update roomtypes
		for (RoomType roomType: city.roomTypeList) {
			updateAvgPricePerRoomType(roomType);
		}
		//update hosts
		for (Host newHost : city.hostList) {
			updateHostParameters(newHost);
		}
	}
	
	private void updateAveragePricePerNight(){
		avgPricePerNight = totalPricePerNight/numberOfRooms;
	}
	
	private void updateAverageReviewsPerMonth(){
		avgNumberOfReviewsPerMonth = ((double) totalReviewsPerMonth)/numberOfRooms;
	}
	
	private void updateEstimatedNumberOfNightsBookedPerYear(){
		estimatedNumberOfNightsBookedPerYear = 12*3*((double) totalReviewsPerMonth)/0.70;
	}
	
	private void updateEstimatedAmountOfMoneySpentPerYear(double numberOfRewviesPerMonth, double price){
		estimatedAmountOfMoneySpentPerYear = 12*3*price*(numberOfRewviesPerMonth)/0.70;
	}
	
	//updates avg price perRoomType for task 3b
	private void updateAvgPricePerRoomType(RoomType newRoomType){
		for (RoomType roomtype : this.roomTypeList) {
			if (roomtype.getRoomType().equals(newRoomType.getRoomType())){
				roomtype.updateAvgRoomTypePrice(newRoomType);
				return;
			}
		}
		roomTypeList.add(newRoomType);
	}
	
	private void updateHostParameters(Host newHost){
		for (Host host : this.hostList) {
			if (host.getId() == newHost.getId()){
				host.updateParameters(newHost);
				return;
			}
		}
		hostList.add(newHost);
	}
	
	public void updateTop3Hosts(){
		top3Hosts = new Host[3];
		double bestHost = -INF;
		double secondBestHost = -INF;
		double thirdBestHost = -INF;
		for (Host host : hostList) {
			double hostIncome = host.getTotalIncome();
			if (hostIncome > bestHost){
				bestHost = hostIncome;
				if (top3Hosts[0] != null){
					secondBestHost = top3Hosts[0].getTotalIncome();	
					top3Hosts[1] = top3Hosts[0];
					if (top3Hosts[1] != null){
						thirdBestHost = top3Hosts[1].getTotalIncome();						
						top3Hosts[2] = top3Hosts[1];
					}
				}
				top3Hosts[0] = host;
			}
			else if (hostIncome > secondBestHost){
				secondBestHost = hostIncome;
				if (top3Hosts[1] != null){
					thirdBestHost = top3Hosts[1].getTotalIncome();
					top3Hosts[2] = top3Hosts[1];					
				}
				top3Hosts[1] = host;
			}
			else if (hostIncome > thirdBestHost){
				thirdBestHost = hostIncome;
				top3Hosts[2] = host;
			}
		}
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
	
	public ArrayList<RoomType> getRoomTypeList(){
		return roomTypeList;
	}
	
	public double getAvgNumberOfReviewsPerMonth() {
		return avgNumberOfReviewsPerMonth;
	}
	
	public double getEstimatedNumberOfNightsBookedPerYear() {
		return estimatedNumberOfNightsBookedPerYear;
	}
	
	public double getEstimatedAmountOfMoneySpentPerYear() {
		return estimatedAmountOfMoneySpentPerYear;
	}
	
	public ArrayList<Host> getHostList() {
		return hostList;
	}
	
	public Host[] getTop3Hosts() {
		return top3Hosts;
	}

}
