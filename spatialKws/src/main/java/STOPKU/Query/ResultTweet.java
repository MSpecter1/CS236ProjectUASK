package STOPKU.Query;

public class ResultTweet {
    private int tid;
    private double score;

    public ResultTweet(int tid , double score)
    {
        this.tid = tid;
        this.score = score;
    }

    public int get_tid()
    {
        return tid;
    }

    public double get_score()
    {
        return score;
    }


    @Override
    public boolean equals(Object obj)
    {
        return tid == ((ResultTweet)obj).tid;
    }

}
