package STOPKU.indexing;

import java.util.Arrays;

public class TweetGeneral {
    private int tweet_id;
    private String[] words;
    private double[] weights;
    public String[] strings;


    public TweetGeneral(){}

    public TweetGeneral(int tweet_id, String[] words, double[] weights, String[] strings)
    {
        this.tweet_id = tweet_id;
        this.words = words;
        this.weights = weights;
        this.strings = strings;
    }

    public int get_tweet_id()
    {
        return tweet_id;
    }

    public String[] get_words()
    {
        return words;
    }

    public double[] get_weights()
    {
        return weights;
    }

    public String[] get_strings()
    {
        return strings;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TweetGeneral tweetGeneral = (TweetGeneral) o;
        return tweet_id == tweetGeneral.tweet_id  && Arrays.equals(words, tweetGeneral.words) && Arrays.equals(weights, tweetGeneral.weights) && Arrays.equals(strings, tweetGeneral.strings);
    }


}
