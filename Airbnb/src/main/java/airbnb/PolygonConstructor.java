package airbnb;

import java.awt.Color;
import java.awt.Dimension;
import java.awt.Graphics;
import java.awt.Polygon;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.awt.geom.Path2D;
import java.awt.geom.Point2D;
import java.util.ArrayList;

import javax.swing.JFrame;
import javax.swing.JPanel;

public class PolygonConstructor {
	
	private Path2D path;
	
	private final ArrayList<Double> latitudes;
	private final ArrayList<Double> longtitudes;
	private final String neighbourhoodGroup;
	private final String neighbourhood;
	
	
	public PolygonConstructor(ArrayList<Double> latitudes, ArrayList<Double> longtitudes, String neighbourhoodGroup, String neighbourhood){
		this.latitudes = latitudes;
		this.longtitudes = longtitudes;
		this.neighbourhoodGroup = neighbourhoodGroup;
		this.neighbourhood = neighbourhood;
		if (latitudes.size() > 1){
			initPath();			
		}
	}
	
	public void initPath() {
        path = new Path2D.Double();

        path.moveTo(longtitudes.get(0), latitudes.get(0));
        for(int i = 1; i < latitudes.size(); ++i) {
           path.lineTo(longtitudes.get(i), latitudes.get(i));
        }
        path.closePath();
    }
	
	public boolean checkIfInsideOfPath(Point2D point){
		return path.contains(point);
	}


	public String getNeighbourhoodGroup() {
		return neighbourhoodGroup;
	}

	public String getNeighbourhood() {
		return neighbourhood;
	}
	
}
