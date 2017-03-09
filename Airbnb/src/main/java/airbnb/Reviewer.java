package airbnb;

import scala.Serializable;

public class Reviewer implements Serializable{
	
	private final String name;
	private final int id;
	private int numberOfBookings;
	private double totalAmountSpentOnAccomodation;
	private int numberOfReviews;
	
	public Reviewer(String name, int id, double price, int number_of_reviews){
		this.name = name;
		this.id = id;
		this.numberOfReviews = number_of_reviews;
		this.totalAmountSpentOnAccomodation += 3*numberOfBookings*price;
	}
	
	public void updateParameters(Reviewer newReviewer){
		this.numberOfReviews += newReviewer.numberOfReviews;
	}
	
	public String getName() {
		return name;
	}

	public int getId() {
		return id;
	}

	public int getNumberOfBookings() {
		return numberOfBookings;
	}

	public double getTotalAmountSpentOnAccomodation() {
		return totalAmountSpentOnAccomodation;
	}
	
	public int getNumberOfReviews(){
		return numberOfReviews;
	}

}
