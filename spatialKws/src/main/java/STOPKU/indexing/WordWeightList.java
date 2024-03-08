package STOPKU.indexing;

import java.util.ArrayList;
import java.util.HashSet;

public class WordWeightList {
    private String word;
    private ArrayList<InvertedListEntry> enties;

    public WordWeightList(){}
    public WordWeightList(String word)
    {
        this.word =word;
        this.enties = new ArrayList<>();
    }


    public String get_word()
    {
        return word;
    }

    public void add(double weight , int id)
    {
        enties.add(new InvertedListEntry(weight , id));
    }

    public void sort()
    {
        enties.sort((o1, o2) -> Double.compare(o2.weight , o1.weight));
    }

    public int get_size()
    {
        return enties.size();
    }

    public double get_greatest()
    {
        return enties.get(0).weight;
    }

    public ArrayList<InvertedListEntry> get_entries()
    {
        return enties;
    }

    public static class InvertedListEntry
    {
        double weight;
        int id;
        public InvertedListEntry(){}
        public InvertedListEntry(double weight, int id)
        {
            this.weight = weight;
            this.id = id;
        }

        public int get_id()
        {
            return id;
        }

    }

    public HashSet<Integer> get_ids()
    {
        HashSet<Integer> hs = new HashSet<>();
        for(InvertedListEntry ile : enties)
        {
            hs.add(ile.get_id());
        }

        return hs;
    }



}
