package airbnb;

import scala.Serializable;

public class Listing implements Serializable{
	
	private final int id;
	private int numberOfAvailable;
	private int numberOfNotAvailable;
	
	public Listing(double id, String available){
		this.id = (int)id;
		if (available.equals("t")){
			numberOfAvailable = 1;
			numberOfNotAvailable = 0;
		}
		else if (available.equals("f")){
			numberOfAvailable = 0;
			numberOfNotAvailable = 1;
		}
		else{
			throw new IllegalArgumentException("Availability is missing");
		}
	}
	
	
	public void updateAvailable(Listing newListing){
		this.numberOfAvailable += newListing.numberOfAvailable;
		this.numberOfNotAvailable += newListing.numberOfNotAvailable;
	}
	
	public int getId() {
		return id;
	}

	public int getNumberOfAvailable() {
		return numberOfAvailable;
	}

	public int getNumberOfNotAvailable() {
		return numberOfNotAvailable;
	}

}
