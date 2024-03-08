package STOPKU.indexing;

public class TweetTexual {
    private int tid;
    private String[] strings;

    public TweetTexual(){}
    public TweetTexual(int tid , String[] strings)
    {
        this.tid = tid;
        this.strings = strings;
    }

    public int get_tid()
    {
        return tid;
    }

    public String[] get_strings()
    {
        return strings;
    }
}
