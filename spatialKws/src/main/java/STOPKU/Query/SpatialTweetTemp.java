package STOPKU.Query;

public class SpatialTweetTemp {
    private int tid;
    private double val;
    public SpatialTweetTemp(int tid , double val)
    {
        this.tid = tid;
        this.val = val;
    }

    public int get_tid()
    {
        return tid;
    }

    public double get_val()
    {
        return val;
    }
}
