package STOPKU.indexing;

public class TweetWeightIndexHashVal {

    double max_weight;
    int indicator;
    int len;
    public TweetWeightIndexHashVal() {}
    public TweetWeightIndexHashVal(double max_weight , int indicator , int len)
    {
        this.max_weight = max_weight;
        this.indicator = indicator;
        this.len = len;
    }

    public double get_max_weight()
    {
        return max_weight;
    }


    public int get_indicator()
    {
        return indicator;
    }

    public int getLen() {return len;}
}
