package STOPKU.indexing;

import java.io.Serializable;

/**
 * This class documents the simplified information of a spatio-textual object, i.e., id and spatial location
 * for quick indexing in spatial dimension
 */
public class SpatialTweet implements Serializable {
    private int oid;
    private double lat;
    private double lon;

    public SpatialTweet() {}
    public SpatialTweet(int oid, double lat, double lon)
    {
        this.oid = oid;
        this.lat = lat;
        this.lon = lon;
    }

    public int get_oid()
    {
        return oid;
    }

    public double get_lat()
    {
        return lat;
    }

    public double get_lon()
    {
        return lon;
    }

    public boolean inside_node(Node n)
    {
        return lat >= n.get_min_lat() && lat <= n.get_max_lat() && lon >= n.get_min_lon() && lon <= n.get_max_lon();
    }

}
