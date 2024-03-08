package STOPKU.disk;

import STOPKU.indexing.*;
import STOPKU.indexing.Node;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.*;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Objects;

import static STOPKU.disk.Tools.kryo;

public class ReadFromDisk {




    public static Quadtree read_quad_tree(int dataset_size , int c , int depth , int spatial_div) throws IOException {

        FlushToDisk.ROOT = FlushToDisk.BASE + (dataset_size / 1000000) + "m" + "c" + c + "d" + depth + "d" + spatial_div;
        FlushToDisk.TREE = FlushToDisk.ROOT + "/TreeNode";
        FlushToDisk.TWEETS = FlushToDisk.ROOT + "/TweetTermWeight";


        Input input = new Input(new FileInputStream(FlushToDisk.TREE + "/tree.bin"));
        Quadtree qt =  kryo.readObject(input, Quadtree.class);
        input.close();

        return qt;
    }


    public static TweetWeight read_tweet_weights_using_index(TweetWeightIndex twi , int node_id , Kryo kryo) throws IOException {
        if(twi == null)
        {
            return null;
        }
        int indicator = twi.get_indicator();
        String word = twi.get_word();

        if(indicator != -1) //indicating this is an infrequent item
        {
            Input input = new Input(new FileInputStream(FlushToDisk.TREE + "/" + node_id + "/InfrequentTweetWeight/" + indicator + ".bin"));
            ArrayList<WordWeightList> wwls = kryo.readObject(input , ArrayList.class);
            input.close();
            for(WordWeightList wwl : wwls)
            {
                if(word.equals(wwl.get_word()))
                {
                    return new TweetWeight(word , wwl);
                }
            }
        }

        else
        {
            Input input = new Input(new FileInputStream(FlushToDisk.TREE + "/" + node_id + "/FrequentTweetWeight/" + word + ".bin"));
            WordWeightList wwl = kryo.readObject(input , WordWeightList.class);
            input.close();

            return new TweetWeight(word , wwl);
        }

        return null;
    }


    public static HashSet<Integer> read_tweet_hashset_using_index(TweetWeightIndex twi , int node_id , Kryo kryo) throws IOException {
        if(twi == null)
        {
            return null;
        }
        int indicator = twi.get_indicator();
        String word = twi.get_word();

        if(indicator != -1) //indicating this is an infrequent item
        {
            Input input = new Input(new FileInputStream(FlushToDisk.TREE + "/" + node_id + "/InfrequentTweetIDSet/" + indicator + ".bin"));
            HashMap<String , HashSet<Integer>> set_map = kryo.readObject(input , HashMap.class);
            input.close();
            return set_map.get(word);
        }

        else
        {
            Input input = new Input(new FileInputStream(FlushToDisk.TREE + "/" + node_id + "/FrequentTweetIDSet/" + word + ".bin"));
            HashMap<String , HashSet<Integer>> set_map = kryo.readObject(input , HashMap.class);
            input.close();
            return set_map.get(word);
        }


    }







    //do not need to verify whether the tid exists, because this function is called only when it is sure that this tweet will be search
    public static TweetTexual read_tweet_textual_using_id(int tid , int file_id , int node_id , Kryo kryo) throws IOException {


        Input input = new Input(new FileInputStream(FlushToDisk.TREE + "/" + node_id + "/TweetTextual/" + file_id + ".bin"));
        ArrayList<TweetTexual> tts = kryo.readObject(input , ArrayList.class);
        input.close();


        for(TweetTexual tt : tts)
        {
            int id = tt.get_tid();
            String[] strings = tt.get_strings();

            if(id == tid)
            {
                return new TweetTexual(id , strings);
            }
        }

        return null;
    }














}
