package STOPKU.disk;

import STOPKU.indexing.*;
import com.esotericsoftware.kryo.Kryo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class Tools {
    public static Kryo kryo = new Kryo();


    public static void init()
    {
        kryo.register(ArrayList .class);
        kryo.register(Quadtree .class);
        kryo.register(Node .class);
        kryo.register(SpatialTweet .class);
        kryo.register(TweetGeneral .class);
        kryo.register(TweetTexual .class);
        kryo.register(TweetTexualIndex.class);
        kryo.register(TweetWeight.class);
        kryo.register(TweetWeightIndex.class);
        kryo.register(TweetWeightIndexHashVal.class);
        kryo.register(String[].class);
        kryo.register(int[].class);
        kryo.register(double[].class);
        kryo.register(HashMap.class);
        kryo.register(Node.GridInNode.class);
        kryo.register(HashSet.class);
        kryo.register(Node[].class);
        kryo.register(WordWeightList.class);
        kryo.register(HashMap.class);
        kryo.register(WordWeightList.InvertedListEntry.class);
    }

    public static void init(Kryo kryo)
    {
        kryo.register(ArrayList .class);
        kryo.register(Quadtree .class);
        kryo.register(Node .class);
        kryo.register(SpatialTweet .class);
        kryo.register(TweetGeneral .class);
        kryo.register(TweetTexual .class);
        kryo.register(TweetTexualIndex.class);
        kryo.register(TweetWeight.class);
        kryo.register(TweetWeightIndex.class);
        kryo.register(TweetWeightIndexHashVal.class);
        kryo.register(String[].class);
        kryo.register(int[].class);
        kryo.register(double[].class);
        kryo.register(HashMap.class);
        kryo.register(Node.GridInNode.class);
        kryo.register(HashSet.class);
        kryo.register(Node[].class);
        kryo.register(WordWeightList.class);
        kryo.register(HashMap.class);
        kryo.register(WordWeightList.InvertedListEntry.class);

    }



}
