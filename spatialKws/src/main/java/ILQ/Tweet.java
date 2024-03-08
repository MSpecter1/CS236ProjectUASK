package ILQ;

import java.util.ArrayList;

public class Tweet {
    private int oid;
    private double lat;
    private double lon;
    private String[] words;


    public Tweet() {}
    public Tweet(int oid, double lat, double lon, String[] words)
    {
        this.oid = oid;
        this.lat = lat;
        this.lon = lon;
        this.words = words;
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

    public String[] get_words()
    {
        return words;
    }

    public boolean inside_node(ILQQuadTreeNode n)
    {
        return lat >= n.get_min_lat() && lat <= n.get_max_lat() && lon >= n.get_min_lon() && lon <= n.get_max_lon();
    }

    public double get_dist(double q_lat , double q_lon)
    {
        return SpatialMetric.compute_dist(lat , lon , q_lat , q_lon);
    }

}
