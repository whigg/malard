
package com.earthwave.pointstream.impl;

import org.gdal.osr.CoordinateTransformation;

public class TransformToXY {
    private double x;
    private double y;

    /**
     * Constructor with lat long input, transforms directly
     * @param lat
     * @param lon
     */
    public TransformToXY(double lat, double lon, CoordinateTransformation transformation){
        this.transform(lat, lon, transformation);
    }

    /**
     * Constructor without transformation input
     */
    public TransformToXY(){
        // do nothing
    }


    /**
     * Method to transform lat and long coordinates
     * Different projection are used depending on where the points lays
     *
     * Projection used can be retrieved with getEpsg()
     * Projected coordinates can be retrieved with getX() and getY()
     *
     * @param lat
     * @param lon
     */
    private void transform(double lat, double lon, CoordinateTransformation transform){

        double [] transformed = transform.TransformPoint(lon, lat);

        this.x = transformed[0];
        this.y = transformed[1];
    }

    /**
     * Get projected x coordinate
     * @return x
     */
    public double getX() {
        return x;
    }

    /**
     * Get projected y coordinate
     * @return y
     */
    public double getY() {
        return y;
    }


}