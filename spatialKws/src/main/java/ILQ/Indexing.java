package ILQ;


import STOPKU.Query.ResultTweet;
import STOPKU.disk.FlushToDisk;
import STOPKU.indexing.*;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.*;
import java.util.*;

import static STOPKU.disk.Tools.kryo;


public class Indexing {



    public static String[] pos_strings = new String[]{"walking" , "dead"};
    public static String[] neg_seq = new String[]{"i" , "back"};
    public static double alpha = 0.001;
    public static double query_lat = 80.1;
    public static double query_lon =  150.1;
    public static int node_capacity = 500;
    public static int depth = 10;
    public static int m = 10;
    public static String ROOT = "/home/gepspatial/Desktop/POWER/SQB-index";
    public static int io_index = 0;
    public static Kryo kryo;

    public static void main(String[] args) throws IOException {
        //new File(ROOT + "/QuadTree").mkdir();
        // /home/gepspatial/Desktop/POWER/SQB-index/tweet/82498.bin (No such file or directory)

        init();
        /*indexing("pdata/tweet4000000" , node_capacity , depth , m);
        System.exit(-1);*/


        QueryProcessor q = new QueryProcessor();
        long start = System.currentTimeMillis();
        PriorityQueue<QueryProcessor.Result> results = q.process_query(query_lat , query_lon , pos_strings , 10);
        long end = System.currentTimeMillis();
        System.out.println("the total runtime is " + (end - start));
        for(QueryProcessor.Result re : results)
        {
            System.out.println(re.tid + " : " + re.dist);
        }


        /*String a = "ab";
        System.out.println(a.substring(0,2));*/
    }




    public static void init()
    {
        kryo = new Kryo();
        kryo.register(ILQ.ILQQuadTreeNode.class);
        kryo.register(ILQ.ILQQuadTreeNode[].class);
        kryo.register(ArrayList .class);
        kryo.register(ILQ.ILQQuadTree.class);
        kryo.register(Tweet.class);
        kryo.register(TweetSpatialWeight.class);
        kryo.register(BPlusTree.class);
        kryo.register(String[].class);
        kryo.register(int[].class);
        kryo.register(BPlusTree.BPTreeLeafNode.class);
        kryo.register(BPlusTree.KeyValuePair[].class);
        kryo.register(BPlusTree.KeyValuePair.class);
        kryo.register(BPlusTree.InternalBPTreeNode.class);
        kryo.register(BPlusTree.BPTreeNode[].class);
        kryo.register(Long[].class);
        kryo.register(HashMap.class);
        System.out.println("init completes");
    }

    public static void indexing(String dir , int capacity , int depth, int m) throws IOException {
        new File(ROOT).mkdir();
        File quad_tree_dir = new File(ROOT + "/QuadTree");
        quad_tree_dir.mkdir();
        File bp_tree_dir = new File(ROOT + "/BPTree");
        bp_tree_dir.mkdir();
        File tweets_dir = new File(ROOT + "/Tweet");
        tweets_dir.mkdir();



        HashMap<String , Integer> word_stat = count_freq(dir);
        HashMap<String , ILQQuadTree> word_quadtree_map = construct_quadtrees(dir , capacity , depth , word_stat);



        flush(word_stat , word_quadtree_map , m);
    }



    public static void flush(HashMap<String ,Integer> word_stat , HashMap<String , ILQQuadTree> word_quadtree_map , int m) throws IOException {

        Output output = new Output(new FileOutputStream(ROOT + "/map.bin"));
        kryo.writeObject(output , word_stat);
        output.close();


        for(String word : word_quadtree_map.keySet())
        {
            ILQQuadTree qt = word_quadtree_map.get(word);
            BPlusTree bpt = construct_single_BPTree(qt , m , word);


            for(ILQQuadTreeNode itn : qt.get_leaf_nodes())
            {
                itn.set_empty();
                itn.clear();
            }


            File f1 = new File(ROOT + "/QuadTree/"  + word + ".bin");
            if(!f1.createNewFile())
            {
                continue;
            }

            output = new Output(new FileOutputStream(f1));
            kryo.writeObject(output , qt);
            output.close();

            File f2 = new File(ROOT + "/BPTree/"  + word + ".bin");
            if(!f2.createNewFile())
            {
                System.out.println("failed in btree , the word is " + word);
                continue;
            }
            output = new Output(new FileOutputStream(f2 ));
            kryo.writeObject(output , bpt);
            output.close();


        }
    }


    public static HashMap<String, Integer> count_freq(String dir) throws IOException {
        HashMap<String , Integer> word_count = new HashMap<>();
        File f_dir = new File(dir);
        BufferedReader reader;
        for(String subfile : Objects.requireNonNull(f_dir.list()))
        {
            reader = new BufferedReader(new FileReader(f_dir + "/" + subfile));
            String line;

            while((line = reader.readLine()) != null && !line.equals(""))
            {
                String[] split_results = line.split(" ");
                int num_words = Integer.parseInt(split_results[3]);
                String[] words = new String[num_words];
                int p = 0;
                for(int j = 4 ; j < 4 + 2 * num_words ; j += 2)
                {
                    words[p] = split_results[j];
                    p++;
                }

                for(String word : words)
                {
                    if(word_count.containsKey(word))
                    {
                        word_count.put(word , word_count.get(word) + 1);
                    }

                    else
                    {
                        word_count.put(word , 1);
                    }
                }


            }
        }




        return word_count;
    }

    public static HashMap<String , ILQQuadTree> construct_quadtrees(String dir , int capacity , int depth , HashMap<String , Integer> word_stat)
    {
        ILQQuadTreeNode.set_depth_capacity(capacity, depth);
        HashMap<String , ILQQuadTree> word_quadtree_map = new HashMap<>(); //each String corresponds to a QuadTree
        File f_dir = new File(dir);
        new File(ILQFlushToDisk.ROOT).mkdir();
        try
        {
            BufferedReader reader = null;
            for(String subfile : Objects.requireNonNull(f_dir.list()))
            {

                reader = new BufferedReader(new FileReader(f_dir + "/" + subfile));
                String line;

                while((line = reader.readLine()) != null && !line.equals(""))
                {
                    String[] split_results = line.split(" ");
                    int tid = Integer.parseInt(split_results[0]);
                    double lat = Double.parseDouble(split_results[1]);
                    double lon = Double.parseDouble(split_results[2]);
                    int num_words = Integer.parseInt(split_results[3]);
                    String[] words = new String[num_words];
                    int p = 0;
                    for(int j = 4 ; j < 4 + 2 * num_words ; j += 2)
                    {
                        words[p] = split_results[j];

                        p++;
                    }

                    //the tree stores the tweet along with the keywords that have lower priority(higher frequency)
                    //the higher the frequency, the lower the probability

                    for (String current_word : words) {
                        //for each get_word insert the tweet into the corresponding quadtree
                        ArrayList<String> t_words = new ArrayList<>();
                        for (String other_word : words) {
                            if (!other_word.equals(current_word)) {
                                if (word_stat.get(other_word) >= word_stat.get(current_word)) //higher frequency, i.e., lower priority
                                {
                                    t_words.add(other_word);
                                }
                            }
                        }

                        t_words.add(current_word);

                        String[] ws = new String[t_words.size()];
                        for (int j = 0; j < t_words.size(); j++) {
                            ws[j] = t_words.get(j);
                        }

                        Tweet tt = new Tweet(tid, lat, lon, ws);


                        if (word_quadtree_map.containsKey(current_word))
                        {
                            word_quadtree_map.get(current_word).insert(tt);
                        }
                        else
                        {
                            ILQQuadTree qt = new ILQQuadTree(current_word);
                            qt.insert(tt);
                            word_quadtree_map.put(current_word, qt);
                        }
                    }





                }
            }

            return word_quadtree_map;

        }

        catch (Exception e)
        {
            e.printStackTrace();
        }

        return null;
    }

    public static BPlusTree construct_single_BPTree(ILQQuadTree qt , int m , String word) throws FileNotFoundException {
        ArrayList<ILQQuadTreeNode> leaf_nodes = qt.get_leaf_nodes();
        BPlusTree bt = new BPlusTree(m);

        for(ILQQuadTreeNode leaf : leaf_nodes)
        {
            if(leaf.get_size() > 0)
            {
                long key;
                if(leaf.get_split_seq().equals(""))
                {
                    key = 0;
                }
                else
                {
                    key = Long.parseLong(leaf.get_split_seq() , 2);
                }

                bt.insert(key , io_index);

                tweetsIO(leaf.get_node_objects() , io_index);

                io_index++;
            }
        }


        return bt;
    }

    public static void tweetsIO(ArrayList<Tweet> tweets , int file_id) throws FileNotFoundException {
        Output output = new Output(new FileOutputStream(ROOT + "/Tweet/" + file_id + ".bin"));
        kryo.writeObject(output , tweets);
        output.close();
        if(io_index == 82498)
        {
            System.out.println("the io index 82498 found");
            File f = new File(ROOT + "/Tweet/" + file_id + ".bin");
            System.out.println(f);
            System.out.println(f.exists());
        }
    }







}
