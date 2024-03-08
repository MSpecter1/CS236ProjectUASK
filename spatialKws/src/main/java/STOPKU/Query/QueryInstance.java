package STOPKU.Query;

import java.util.ArrayList;

public class QueryInstance {
    private double lat;
    private double lon;
    private String[] pos_keywords;
    private ArrayList<String[]> neg_keywords;
    private double alpha;
    private int k;
    private String[] or_keywords;
    private double threshold;

    public QueryInstance(double lat , double lon , String[] pos_keywords , ArrayList<String[]> neg_keywords, double alpha , int k , String[] or_keywords, double threshold)
    {
        this.lat = lat;
        this.lon = lon;
        this.pos_keywords = pos_keywords;
        this.neg_keywords = neg_keywords;
        this.alpha = alpha;
        this.k = k;
        this.or_keywords = or_keywords;
        this.threshold = threshold;
    }





    public double get_lat()
    {
        return lat;
    }

    public double get_lon()
    {
        return lon;
    }

    public String[] get_pos_keywords()
    {
        return pos_keywords;
    }

    public ArrayList<String[]> get_neg_keywords()
    {
        return neg_keywords;
    }

    public int get_k()
    {
        return k;
    }

    public double get_alpha()
    {
        return alpha;
    }

    public double get_threshold()
    {
        return threshold;
    }

    public String[] get_or_keywords() {return or_keywords;}


}
