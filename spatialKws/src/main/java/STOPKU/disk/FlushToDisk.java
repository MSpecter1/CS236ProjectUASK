package STOPKU.disk;

import STOPKU.indexing.*;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.*;

import static STOPKU.disk.Tools.kryo;

public class FlushToDisk {
    private static final int batch_size = 10000;
    private static TweetGeneral[] tweets = new TweetGeneral[batch_size];
    public static int counter = 0;
    // public static String BASE = "/home/gepspatial/Desktop/POWER/POWER-index/";
    public static String BASE = "C:/Users/Michael He/Documents/GitHub/CS236ProjectUASK2/UASK UPDATED CODE/UASK UPDATED CODE/flushpath/";
    public static String ROOT;
    public static String TREE;// = ROOT + "/TreeNode";
    public static String TWEETS;// = ROOT +"/TweetTermWeight";
    public static int master_index = 0;
    public static int tweet_block_id = 0;
    public static final int page = 4 * 1024; //4kb data
    public static final int block = 128 * 1024 * 1024; //a block is 128mb
    public static final int page_per_block = block / page;


    //the function flush the tree structure to the disk
    public static void flush_quad_tree(Quadtree qt , int batch_size , int division_num) throws IOException
    {
        Hashtable<Integer , int[]> id2node = new Hashtable<>();
        Hashtable<Integer , Integer> nid2currentsize = new Hashtable<>();


        for(Node n : qt.get_nodes())
        {

            if(n.is_leaf())
            {
                //System.out.println("traversing " + n.get_node_id() + " size is " + n.get_size());
                /*if(n.get_size() == 0)
                {
                    System.out.println("zero");
                }*/

                double lat_range = n.get_max_lat() - n.get_min_lat();
                double sub_division_len_lat = lat_range / division_num;
                double lon_range = n.get_max_lon() - n.get_min_lon();
                double sub_division_len_lon = lon_range / division_num;

                nid2currentsize.put(n.get_node_id() , n.get_size());
                int obj_num = n.get_size();
                ArrayList<SpatialTweet> objs = n.get_nodes_objects_arr();

                HashMap<Integer , SpatialTweet> id_to_spatial = new HashMap<>();
                HashMap<Integer, Node.GridInNode> grid_map = new HashMap<>();
                for(int i = 0 ; i < obj_num ; i++)
                {
                    SpatialTweet st = objs.get(i);
                    int oid = st.get_oid();
                    double lon = st.get_lon();
                    double lat = st.get_lat();
                    if(lon == n.get_max_lon())
                    {
                        lon = lon - sub_division_len_lon/ 1000;
                    }
                    if(lat == n.get_max_lat())
                    {
                        lat = lat - sub_division_len_lat / 1000;
                    }
                    int x = (int) ((lon - n.get_min_lon()) / sub_division_len_lon);
                    int y = (int) ((lat - n.get_min_lat()) / sub_division_len_lat);

                    int gid = x + (y * division_num);
                    id_to_spatial.put(oid , st);
                    if(grid_map.containsKey(gid))
                    {
                        grid_map.get(gid).add(st);
                    }

                    else
                    {
                        double min_lat = n.get_min_lat() + sub_division_len_lat * y;
                        double max_lat = min_lat + sub_division_len_lat;

                        double min_lon = n.get_min_lon() + sub_division_len_lon * x;
                        double max_lon = min_lon + sub_division_len_lon;


                        Node.GridInNode gn = new Node.GridInNode(gid , max_lat , min_lat , max_lon , min_lon);
                        gn.add(st);
                        grid_map.put(gid , gn);
                    }

                    id2node.put(oid , new int[]{n.get_node_id() , i});
                }

                objs.clear();

                n.map_neigh_node_to_id();

                new File(FlushToDisk.TREE + "/" + n.get_node_id()).mkdir();

                Output output = new Output(new FileOutputStream(FlushToDisk.TREE + "/" + n.get_node_id() + "/SpatialMap.bin" ));
                kryo.writeObject(output, id_to_spatial);
                output.close();

                Input input = new Input(new FileInputStream(FlushToDisk.TREE + "/" + n.get_node_id() + "/SpatialMap.bin" ));
                kryo.readObject(input , HashMap.class);
                input.close();


                output = new Output(new FileOutputStream(FlushToDisk.TREE + "/" + n.get_node_id() + "/Grids.bin" ));
                kryo.writeObject(output , grid_map); //HashMap<Integer, Node.GridInNode>
                output.close();
            }
        }

        System.gc();


        revisit_entry(id2node , nid2currentsize , FlushToDisk.TWEETS  ,  batch_size , qt);
    }


    //data path is FlushToDisk.TWEETS
    public static void revisit_entry(Hashtable<Integer , int[]> id2node  , Hashtable<Integer , Integer> node2currentsize , String data_path , int batch_size , Quadtree qt) throws IOException {
        Hashtable<Integer, TweetGeneral[]> nid2tw = new Hashtable<>();
        

        TweetGeneral[] tweets_batch = new TweetGeneral[batch_size];
        int index = 0;

        for(File sub_dir : new File(data_path).listFiles())
        {
            for(File data_file : sub_dir.listFiles())
            {
                Input input = new Input(new FileInputStream(data_file));
                ArrayList<TweetGeneral> page_tgs = kryo.readObject(input , ArrayList.class);
                input.close();

                for(TweetGeneral tg : page_tgs)
                {
                    tweets_batch[index++] = tg;

                    if(index == batch_size)
                    {
                        for(TweetGeneral twg : tweets_batch)
                        {
                            int tid = twg.get_tweet_id();

                            int[] ret = id2node.get(tid);
                            if(ret == null)
                            {
                                System.out.println("index is " + index + " tweet batch size is " + tweets_batch.length + " the id is " + tid);
                            }
                            int nid = ret[0];
                            int index_in_node = ret[1];

                            if(!nid2tw.containsKey(nid))
                            {
                                TweetGeneral[] tgs = new TweetGeneral[node2currentsize.get(nid)];
                                tgs[index_in_node] = twg;
                                nid2tw.put(nid , tgs);
                            }

                            else
                            {
                                TweetGeneral[] tgs = nid2tw.get(nid);
                                tgs[index_in_node] = twg;
                            }

                            int current_size = node2currentsize.get(nid);
                            current_size--;
                            if(current_size == 0)
                            {
                                flush_leaf_node_all(nid , nid2tw.get(nid), qt);
                                nid2tw.remove(nid);
                                node2currentsize.remove(nid);
                            }

                            else
                            {
                                node2currentsize.put(nid , current_size);
                            }
                        }

                        index = 0;
                        tweets_batch = new TweetGeneral[batch_size];
                        System.gc();
                    }
                }
            }
        }

        if(index > 0)
        {
            //TweetGeneral twg : tweets_batch
            for(int i = 0 ; i < index ; i ++)
            {
                TweetGeneral twg = tweets_batch[i];

                int tid = twg.get_tweet_id();

                int[] ret = id2node.get(tid);
                int nid = ret[0];
                int index_in_node = ret[1];
                if(!nid2tw.containsKey(nid))
                {
                    /*TweetGeneral[] tgs_node = new TweetGeneral[node2currentsize.get(n)];
                    node2tw.put(n , tgs_node);*/
                    TweetGeneral[] tgs = new TweetGeneral[node2currentsize.get(nid)];
                    tgs[index_in_node] = twg;
                    nid2tw.put(nid , tgs);
                }

                else
                {
                    TweetGeneral[] tgs = nid2tw.get(nid);
                    tgs[index_in_node] = twg;
                }

                int current_size = node2currentsize.get(nid);
                current_size--;

                if(current_size == 0)
                {
                    flush_leaf_node_all(nid , nid2tw.get(nid) , qt);
                    nid2tw.remove(nid);
                    node2currentsize.remove(nid);
                }

                else
                {
                    node2currentsize.put(nid , current_size);
                }
            }
        }


        Output output = new Output(new FileOutputStream(FlushToDisk.TREE + "/tree.bin" ));
        kryo.writeObject(output , qt);
        output.close();
    }








    public static void flush_leaf_node_all(int nid , TweetGeneral[] tweets_general , Quadtree qt) throws IOException {
        String leaf_nodes_dir = FlushToDisk.TREE + "/" + nid;
        //System.out.println("current flushing " + nid);

        //word2idweights stores for each keyword the corresponding inverted list

        //the following block constructs the inverted list for each keyword
        HashMap<String , WordWeightList> word2word_weight_list = new HashMap<>();

        for (TweetGeneral tg : tweets_general)
        {
            int id = tg.get_tweet_id();
            for (int j = 0; j < tg.get_weights().length; j++)
            {
                String word = tg.get_words()[j];
                double weight = 0;
                weight  = tg.get_weights()[j];


                if (word2word_weight_list.containsKey(word))
                {
                    WordWeightList wwl = word2word_weight_list.get(word);
                    wwl.add(weight , id);
                }

                else
                {
                    WordWeightList wwl = new WordWeightList(word);
                    wwl.add(weight , id);
                    word2word_weight_list.put(word , wwl);
                }
            }
        }

        //block ends



        //twis is the indexing for tweetweight
        //the following block writes the inverted list for each keyword to the disk


        int currently_occupied_bytes = 0;
        int current_infreq_tweet_weight_index = 0;

        ArrayList<WordWeightList> page_data_wwl = new ArrayList<>();
        HashMap<String , TweetWeightIndexHashVal> weight_index = new HashMap<>();
        for(String word : word2word_weight_list.keySet())
        {
            WordWeightList wwl = word2word_weight_list.get(word);
            wwl.sort();
            int list_size = 0;
            list_size = wwl.get_size() * 12 /* a double and an int */ + 8 + word.length();

            int freq_ind;


            if(list_size  < page) //suggesting that this is an infrequent item
            {
                if(currently_occupied_bytes + list_size >= page) //start a new file to record this information
                {
                    File f = new File(leaf_nodes_dir +"/InfrequentTweetWeight" );
                    if(!f.exists())
                    {
                        f.mkdir();
                    }
                    Output output = new Output(new FileOutputStream(leaf_nodes_dir + "/InfrequentTweetWeight/" + current_infreq_tweet_weight_index + ".bin"));
                    kryo.writeObject(output, page_data_wwl);
                    output.close();

                    //newly added
                    HashMap<String , HashSet<Integer>> keyset_map = new HashMap<>();
                    for(WordWeightList wwlt : page_data_wwl)
                    {
                        String term = wwlt.get_word();
                        keyset_map.put(term , wwlt.get_ids());
                        f = new File(leaf_nodes_dir +"/InfrequentTweetIDSet" );
                        if(!f.exists())
                        {
                            f.mkdir();
                        }
                        output = new Output(new FileOutputStream(leaf_nodes_dir + "/InfrequentTweetIDSet/" + current_infreq_tweet_weight_index + ".bin"));
                        kryo.writeObject(output, keyset_map);
                        output.close();


                    }
                    //newly added

                    current_infreq_tweet_weight_index++;
                    page_data_wwl.clear();
                    currently_occupied_bytes = 0;
                }

                currently_occupied_bytes += list_size;
                page_data_wwl.add(wwl);

                freq_ind = current_infreq_tweet_weight_index;

                //remember to flush the rest of the data available
            }

            else //suggesting this is a frequent item
            {
                freq_ind = -1;
                /*block_write_buffer.write(size);
                for (InvertedListComp wi : wis)
                {
                    block_write_buffer.write(wi.get_id());
                    block_write_buffer.write(wi.get_weight());

                }*/
                File f = new File(leaf_nodes_dir + "/FrequentTweetWeight");
                if(!f.exists())
                {
                    f.mkdir();
                }
                Output output = new Output(new FileOutputStream(leaf_nodes_dir + "/FrequentTweetWeight/" + word + ".bin"));

                kryo.writeObject(output, wwl);
                output.close();

                //newly added
                HashMap<String , HashSet<Integer>> keyset_map = new HashMap<>();
                keyset_map.put(wwl.get_word() , wwl.get_ids());
                f = new File(leaf_nodes_dir +"/FrequentTweetIDSet" );
                if(!f.exists())
                {
                    f.mkdir();
                }
                output = new Output(new FileOutputStream(leaf_nodes_dir + "/FrequentTweetIDSet/" + word + ".bin"));
                kryo.writeObject(output, keyset_map);
                output.close();

                //newly added


            }

            weight_index.put(word , new TweetWeightIndexHashVal(wwl.get_greatest() , freq_ind , wwl.get_size()));

        }

        if(!page_data_wwl.isEmpty())
        {
            File f = new File(leaf_nodes_dir +"/InfrequentTweetWeight" );
            if(!f.exists())
            {
                f.mkdir();
            }
            Output output = new Output(new FileOutputStream(leaf_nodes_dir + "/InfrequentTweetWeight/" + current_infreq_tweet_weight_index + ".bin"));
            kryo.writeObject(output, page_data_wwl);
            output.close();


            //newly added
            HashMap<String , HashSet<Integer>> keyset_map = new HashMap<>();
            for(WordWeightList wwlt : page_data_wwl)
            {
                String term = wwlt.get_word();
                keyset_map.put(term , wwlt.get_ids());
                f = new File(leaf_nodes_dir +"/InfrequentTweetIDSet" );
                if(!f.exists())
                {
                    f.mkdir();
                }
                output = new Output(new FileOutputStream(leaf_nodes_dir + "/InfrequentTweetIDSet/" + current_infreq_tweet_weight_index + ".bin"));
                kryo.writeObject(output, keyset_map);
                output.close();
            }

            page_data_wwl.clear();
            //newly added
        }
        //block ends


        qt.get_node_by_id(nid).set_weight_index(weight_index);

        HashMap<Integer , Integer> id2file = new HashMap<>();
        currently_occupied_bytes = 0;
        int current_tweet_textual_file = 0;
        ArrayList<TweetTexual> page_tt = new ArrayList<>();
        for(TweetGeneral tw : tweets_general)
        {
            int size = 4 /* id */ + 4 /* length of string */ + 4 /* length for each string */ * (tw.get_strings().length);
            for(String str : tw.get_strings())
            {
                size += str.length();
            }

            if(currently_occupied_bytes + size >= page)
            {
                File f = new File(leaf_nodes_dir +"/TweetTextual");
                if(!f.exists())
                {
                    f.mkdir();
                }
                Output output = new Output(new FileOutputStream(leaf_nodes_dir + "/TweetTextual/" + current_tweet_textual_file++ + ".bin"));
                kryo.writeObject(output, page_tt);
                output.close();
                page_tt.clear();
                currently_occupied_bytes = 0;
            }


            page_tt.add(new TweetTexual(tw.get_tweet_id() , tw.get_strings()));

            id2file.put(tw.get_tweet_id() , current_tweet_textual_file);
            currently_occupied_bytes += size;
        }

        if(!page_tt.isEmpty())
        {
            File f = new File(leaf_nodes_dir +"/TweetTextual");
            if(!f.exists())
            {
                f.mkdir();
            }
            Output output = new Output(new FileOutputStream(leaf_nodes_dir + "/TweetTextual/" + current_tweet_textual_file++ + ".bin"));
            kryo.writeObject(output, page_tt);
            output.close();
            page_tt.clear();
        }


        qt.get_node_by_id(nid).set_textual_index(id2file);



    }


    public static void insert_tweets(TweetGeneral t) throws IOException {
        tweets[counter++] = t;
        if(counter == batch_size)
        {
            flush_tweets_disk();
            tweets = new TweetGeneral[batch_size];
            counter = 0;
        }
    }

    public static void flush_tweets_disk() throws IOException {
        int currently_occupied_bytes = 0;
        String current_dir = FlushToDisk.TWEETS + "/" + FlushToDisk.tweet_block_id;
        ArrayList<TweetGeneral> page_data = new ArrayList<>();
        for(TweetGeneral tt : tweets)
        {
            if(tt == null)
            {
                break;
            }
            int num_bytes = 4 /*tweet id*/ + 4 /*num of words*/  + 4 + 8 * tt.get_words().length; /*for each get_word-weight, we store an integer(number of bytes) and a double(weight)*/
            for(String str : tt.get_words())
            {
                num_bytes += (str.getBytes().length  + 4);
            }

            for(String str : tt.get_strings())
            {
                num_bytes += (str.getBytes().length  + 4);
            }

            if(currently_occupied_bytes + num_bytes >= page)
            {
                if(FlushToDisk.master_index >= page_per_block)
                {
                    FlushToDisk.tweet_block_id++;
                    current_dir = FlushToDisk.TWEETS + "/" + FlushToDisk.tweet_block_id;
                    FlushToDisk.master_index = 0;
                }

                File f = new File(current_dir);
                if(!f.exists())
                {
                    f.mkdir();
                }

                Output output = new Output(new FileOutputStream(current_dir + "/" + FlushToDisk.master_index++ + ".bin"));
                kryo.writeObject(output, page_data);
                output.close();
                currently_occupied_bytes = 0;
                page_data.clear();
            }

            currently_occupied_bytes += num_bytes;
            page_data.add(tt);
        }

        //write the rest of the tweets to the file

        if(!page_data.isEmpty())
        {
            if(FlushToDisk.master_index >= page_per_block)
            {
                FlushToDisk.tweet_block_id++;
                current_dir = FlushToDisk.TWEETS + "/" + FlushToDisk.tweet_block_id;
                FlushToDisk.master_index = 0;
            }

            File f = new File(current_dir);
            if(!f.exists())
            {
                f.mkdir();
            }

            Output output = new Output(new FileOutputStream(current_dir + "/" + FlushToDisk.master_index++ + ".bin"));
            kryo.writeObject(output, page_data);
            output.close();
            page_data.clear();



        }





    }









}
