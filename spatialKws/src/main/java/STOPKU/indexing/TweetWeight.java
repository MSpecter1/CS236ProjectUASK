package STOPKU.indexing;

import java.util.*;

public class TweetWeight {
    private String word;
    WordWeightList wwl;
    private HashMap<Integer , Double> hashmap;

    public TweetWeight() {}
    public TweetWeight(String word , WordWeightList wwl)
    {
        this.word = word;
        this.wwl = wwl;
        this.hashmap = new HashMap<>();
        for(WordWeightList.InvertedListEntry ile : wwl.get_entries())
        {
            hashmap.put(ile.id , ile.weight);
        }
    }



    public int get_len()
    {
        return wwl.get_size();
    }

    public double get_weights(int i)
    {
        return wwl.get_entries().get(i).weight;
    }

    public int get_tids(int i)
    {
        return wwl.get_entries().get(i).id;
    }

    public HashMap<Integer , Double> get_hash()
    {
        return hashmap;
    }






}
