package ILQ;

import STOPKU.indexing.Node;

public class TweetSpatialWeight {
    private int oid;
    private double lat;
    private double lon;

    private double weight;

    public TweetSpatialWeight() {}

    public TweetSpatialWeight(int oid, double lat, double lon,  double weight)
    {
        this.oid = oid;
        this.lat = lat;
        this.lon = lon;
        this.weight = weight;
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


    public double get_weight()
    {
        return weight;
    }


    public boolean inside_node(ILQQuadTreeNode n)
    {
        return lat >= n.get_min_lat() && lat <= n.get_max_lat() && lon >= n.get_min_lon() && lon <= n.get_max_lon();
    }
}
