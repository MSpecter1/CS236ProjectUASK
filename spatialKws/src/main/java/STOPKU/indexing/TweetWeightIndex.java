package STOPKU.indexing;

//this is the tweetWeigtIndex that is being built when reading the index file
//the index files are sorted in dictionary order. Each index file takes 4 KB storage
//if the indicator is -1, that means it is a frequent get_word
//In this situation, we look into TreeNode/No./FrequenTweet and use the string as idex
//if the indicator is not -1, that means it is a non-frequent get_word.
//In this situation, we look into TreeNode/No,/InfrequentTweet and use the indicator read
//At last a TweetWeight Object will be retuened to the user
public class TweetWeightIndex {
    private String word;
    private int indicator; //this value is set to 0 if it is a frequent get_word and non-0(the actual file name) if it is non-frequent
    private double max_weight;

    public TweetWeightIndex() {}
    public TweetWeightIndex(String word , int indicator, double max_weight)
    {
        this.word = word;
        this.indicator = indicator;
        this.max_weight = max_weight;
    }

    public TweetWeightIndex(String word , int indicator)
    {
        this.word = word;
        this.indicator = indicator;
    }

    public String get_word()
    {
        return word;
    }

    public int get_indicator()
    {
        return indicator;
    }

    public double get_max_weight()
    {
        return max_weight;
    }


}
