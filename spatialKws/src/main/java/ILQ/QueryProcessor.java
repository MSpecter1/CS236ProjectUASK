package ILQ;



import STOPKU.disk.FlushToDisk;
import STOPKU.disk.Tools;
import STOPKU.indexing.Quadtree;
import STOPKU.indexing.WordWeightList;
import com.esotericsoftware.kryo.io.Input;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.*;

import static ILQ.Indexing.ROOT;
import static ILQ.Indexing.kryo;

public class QueryProcessor {

    public HashMap<String , Integer> word_stat;


    public QueryProcessor() throws FileNotFoundException {
        this.word_stat = new HashMap<>();
        read_index();
    }




    public void read_index() throws FileNotFoundException {
        /*for(String subfile : Objects.requireNonNull(new File(ROOT + "/QuadTree").list()))
        {
            String word = subfile.substring(0 , subfile.length() - 4);

            Input input = new Input(new FileInputStream(ROOT + "/QuadTree/" + subfile));
            ILQQuadTree qt =  kryo.readObject(input, ILQQuadTree.class);
            input.close();
            word_quadtree_map.put(word , qt);
        }*/

        /*for(String subfile : Objects.requireNonNull(new File(ROOT + "/BPTree").list()))
        {
            String word = subfile.substring(0 , subfile.length() - 4);

            Input input = new Input(new FileInputStream(ROOT + "/BPTree/" + subfile));
            BPlusTree bpt =  kryo.readObject(input, BPlusTree.class);
            input.close();
            word_bplustree.put(word , bpt);
        }*/

        Input input = new Input(new FileInputStream(ROOT + "/map.bin"));
        this.word_stat =  kryo.readObject(input, HashMap.class);
        input.close();
    }

    public PriorityQueue<Result> process_query(double lat , double lon , String[] q_words , int k) throws FileNotFoundException {
        String highest_word = find_highest_prioirity(q_words);
        //System.out.println("the highest word is " + highest_word);

        if(highest_word == null)
        {
            return null;
        }
        else
        {
            long start = System.currentTimeMillis();
            Input input = new Input(new FileInputStream(ROOT + "/QuadTree/" + highest_word + ".bin"));
            ILQQuadTree quad_tree =  kryo.readObject(input, ILQQuadTree.class);
            input.close();

            input = new Input(new FileInputStream(ROOT + "/BPTree/" + highest_word + ".bin"));
            BPlusTree bplus_tree =  kryo.readObject(input, BPlusTree.class);
            input.close();
            long end = System.currentTimeMillis();
            //System.out.println("io takes " + (end - start));

            return incremental_search(quad_tree , bplus_tree , lat , lon , q_words , k);
        }

    }

    public PriorityQueue<Result> incremental_search(ILQQuadTree qt , BPlusTree bt , double q_lat , double q_lon , String[] q_words , int k) throws FileNotFoundException {
        //keep it size k, and the popped entry has the minimum score(greatest distance) among the current top-k
        PriorityQueue<Result> results = new PriorityQueue<>((o1, o2) -> Double.compare(o2.dist, o1.dist));
        double kth_distance = Double.MAX_VALUE;



        PriorityQueue<QueueEntry> pq = new PriorityQueue<>(Comparator.comparingDouble(o -> o.score));

        if(qt.get_root().is_leaf())
        {
            pq.add(new QueueEntry(0 , qt.get_root().min_dist_to_node(q_lat ,  q_lon) , qt.get_root()));
        }

        else
        {
            pq.add(new QueueEntry(1 , qt.get_root().min_dist_to_node(q_lat , q_lon) , qt.get_root()));
        }

        while(!pq.isEmpty())
        {
            QueueEntry e = pq.poll();

            if(e.cat == 0 ) //it is a non-empty leaf quadrant
            {
                //System.out.println("it is a non-empty leaf");
                long search_key;
                if(e.n.get_split_seq().equals(""))
                {
                    search_key = 0;
                }
                else
                {
                    search_key = Long.parseLong(e.n.get_split_seq() , 2);
                }
                //System.out.println("the split sequence is " + e.n.get_split_seq() + " empty? " + e.n.is_empty());
                int fid = bt.search(search_key);
                //System.out.println("the search key is " + search_key);
                Input input = new Input(new FileInputStream(ROOT + "/Tweet/" + fid + ".bin"));
                ArrayList<Tweet> ts = kryo.readObject(input , ArrayList.class);
                input.close();
                for(Tweet t : ts)
                {
                    String[] words = t.get_words();
                    HashSet<String> words_map = new HashSet<>(Arrays.asList(words));
                    int hit = 0;
                    for(String q_word : q_words)
                    {
                        if(words_map.contains(q_word))
                        {
                            hit++;
                        }
                    }

                    if(hit == q_words.length) //meaning that it contains all the query kws
                    {
                        double dist = t.get_dist(q_lat , q_lon);

                        if(results.size() < k)
                        {
                            results.add(new Result(t.get_oid() , dist));
                            kth_distance = results.peek().dist;
                        }

                        else //meaning that the results current have size k
                        {
                            if(dist < kth_distance)
                            {
                                results.poll();
                                results.add(new Result(t.get_oid() , dist));
                                kth_distance = results.peek().dist;
                            }
                        }

                    }
                }
            }

            else //otherwise, meaning it is not a leaf node
            {
                //System.out.println("it is not a leaf node");
                for(ILQQuadTreeNode n : e.n.get_child_nodes())
                {
                    double min_dist = n.min_dist_to_node(q_lat , q_lon);
                    if(min_dist < kth_distance)
                    {
                        //System.out.println("smaller than kth dist");
                        if(n.is_leaf()) //consider the non-leaf nodes
                        {
                            if(!n.is_empty())
                            {
                                pq.add(new QueueEntry(0 , min_dist , n));
                            }
                        }

                        else
                        {
                            pq.add(new QueueEntry(1 , min_dist , n));
                        }
                    }
                }
            }
        }
        //System.out.println("the first poped entry is " + results.peek().tid + " dist is " + results.peek().dist);

        return results;

    }

    class Result
    {
        int tid;
        double dist;
        public Result(int tid , double dist)
        {
            this.tid = tid;
            this.dist = dist;
        }
    }

    class QueueEntry
    {
        int cat;// 0 means it is non-empty leaf quadrant , and 0 means otherwise
        double score;// the distance, the smaller means
        ILQQuadTreeNode n;
        public QueueEntry(int cat , double score , ILQQuadTreeNode n)
        {
            this.cat = cat;
            this.score = score;
            this.n = n;
        }
    }

    public String find_highest_prioirity(String[] words)
    {
        String highest_pri_word = null;
        int min_freq = Integer.MAX_VALUE;
        for(String word : words)
        {
            if(!word_stat.containsKey(word)) //indicating that some input keyword is not included in any tweet
            {
                return null;
            }

            else
            {
                int freq = word_stat.get(word);
                if(freq < min_freq)
                {
                    min_freq = freq;
                    highest_pri_word = word;
                }
            }
        }
        return highest_pri_word;
    }

}
