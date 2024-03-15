package STOPKU.Query;

import STOPKU.disk.FlushToDisk;
import STOPKU.disk.ReadFromDisk;
import STOPKU.disk.Tools;
import STOPKU.indexing.*;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;



public class OptQueryProcessor {

    private Quadtree qt;
    private int num_thread;
    private long spatial_buffer_size;
    private Hashtable<Integer , SpatialBufferEntry> spatial_buffer;
    private long current_spatial_buffer;
    private Kryo[] kryos;

    public OptQueryProcessor(Quadtree qt , int num_thread , long spatial_buffer_size)
    {
        this.qt = qt;
        this.num_thread = num_thread;
        this.spatial_buffer_size = spatial_buffer_size;

        spatial_buffer = new Hashtable<>();
        current_spatial_buffer = 0;
        kryos = new Kryo[num_thread];
        for (int i = 0 ; i < num_thread ; i++)
        {
            kryos[i] = new Kryo();
            Tools.init(kryos[i]);
        }
    }
    
    public ArrayList<ResultTweet> process_query_range(QueryInstanceRange qi) {
        double lon1 = qi.get_lon1();
        double lat1 = qi.get_lat1();
        double lon2 = qi.get_lon2();
        double lat2 = qi.get_lat2();
        String[] pos_keywords = qi.get_pos_keywords();
        ArrayList<String[]> neg_keywords = qi.get_neg_keywords();
        int k = qi.get_k();
        String[] or_keywords = qi.get_or_keywords();
        double alpha = 0;
        double threshold = 0;

        if(or_keywords == null)
        {
            alpha = qi.get_alpha();
            threshold = qi.get_threshold();
        }

        HashSet<Node> processed = new HashSet<>();
        // make new prio queue
        PriorityQueue<Node> processing_nodes = new PriorityQueue<>(Comparator.comparingDouble(o -> Node.min_dist_to_node(qi.get_lat1(), qi.get_lon1() , o)));
        // search to point query, should search until lat2, lon2
        processing_nodes.add(qt.search_path_to_leaf(new SpatialTweet(-1 , lat1 , lon1)));

        // PUT ALL RESULTS INSIDE ARRAYLIST:
        ArrayList<ResultTweet> results = new ArrayList<>();

        LocalThreadRange[] threads = new LocalThreadRange[num_thread];
        boolean[] busy = new boolean[num_thread];
        Queue<ArrayList<ResultTweet>> loc_results = new LinkedList<>();
        ReentrantLock lock = new ReentrantLock();

        boolean break_flag = false;
        // for (String x : neg_keywords.get(0)){
        //     System.out.println(x);
        // }

        while(true)
        {
            for(int i = 0 ; i < num_thread ; i++)
            {
                if(processing_nodes.size() == 0)
                {
                    break_flag = true;
                    break;
                }

                if(!busy[i])
                {
                    // get frontier node and pop
                    Node n = processing_nodes.remove();
                    // System.out.println("the processing node is " + n.get_node_id());
                    processed.add(n);
                    //the following adds neighbors to the current
                    for(int neigh_nid : n.get_neighbor_ids())
                    {
                        Node neigh_n = qt.get_node_by_id(neigh_nid);
                        // add to frontier if neighbor has not been processed
                        if(!processed.contains(neigh_n) && !processing_nodes.contains(neigh_n))
                        {
                            // check for overlap with range query
                            if (!((neigh_n.get_min_lat()>lat2) || (neigh_n.get_max_lat()<lat1) || (neigh_n.get_min_lon()>lon2) || (neigh_n.get_max_lon()<lon1)))
                            {
                                processing_nodes.add(neigh_n);
                            }
                        }
                    }

                    if(n.get_size() == 0)
                    {
                        continue;
                    }
                    double max_spatial_score = 1 - Node.min_dist_to_node(lat1 , lon1, n) / Quadtree.MAX_SPATIAL_DIST;
                    // Calls new thread to check if each leaf node has suitable results, and adds to result list
                    // threads[i] = new LocalThreadRange(n , k , lat1, lat2, lon1, lon2 , pos_keywords , neg_keywords , alpha , lock , loc_results , busy , i , null ,  results , or_keywords , threshold , kryos[i]);
                    threads[i] = new LocalThreadRange(n , k , lat1, lat2, lon1, lon2 , pos_keywords , neg_keywords , alpha , lock , loc_results , busy , i , max_spatial_score ,  results , or_keywords , threshold , kryos[i]);
                    threads[i].start();

                }
            }

            if(break_flag)
            {
                //System.out.println("broken from the flag");
                break;
            }
        }

        //System.out.println("the first while break");
        while(true)
        {
            boolean flag = true;
            for (boolean b : busy) {
                if (b) {
                    flag = false;
                     //Thread.sleep(0, 1);
                    Thread.yield();
                    //System.out.println("false index " + count);
                    break;
                }

            }

            if(flag)
            {
                //System.out.println("thread yield called");
                Thread.yield();
                break;
            }
        }

        while(loc_results.size() > 0)
        {
            ArrayList<ResultTweet> local_result = loc_results.remove();
            for(ResultTweet rt : local_result)
            {
                results.add(rt);
            }
        }

        return results;

    }

    public PriorityQueue<ResultTweet> process_query(QueryInstance qi) {
        double lon = qi.get_lon();
        double lat = qi.get_lat();
        String[] pos_keywords = qi.get_pos_keywords();
        ArrayList<String[]> neg_keywords = qi.get_neg_keywords();
        int k = qi.get_k();
        String[] or_keywords = qi.get_or_keywords();
        double alpha = 0;
        double threshold = 0;

        if(or_keywords == null)
        {
            alpha = qi.get_alpha();
            threshold = qi.get_threshold();
        }




        HashSet<Node> processed = new HashSet<>();
        PriorityQueue<Node> processing_nodes = new PriorityQueue<>(Comparator.comparingDouble(o -> Node.min_dist_to_node(qi.get_lat(), qi.get_lon() , o)));
        processing_nodes.add(qt.search_path_to_leaf(new SpatialTweet(-1 , lat , lon)));
        PriorityQueue<ResultTweet> global_topk = new PriorityQueue<>(Comparator.comparingDouble(ResultTweet::get_score));

        LocalThread[] threads = new LocalThread[num_thread];
        boolean[] busy = new boolean[num_thread];
        Queue<PriorityQueue<ResultTweet>> topk_results = new LinkedList<>();
        ReentrantLock lock = new ReentrantLock();

        boolean break_flag = false;
        while(true)
        {
            for(int i = 0 ; i < num_thread ; i++)
            {
                if(processing_nodes.size() == 0)
                {
                    break_flag = true;
                    break;
                }

                if(!busy[i])
                {
                    Node n = processing_nodes.remove();
                    //System.out.println("the processing node is " + n.get_node_id());
                    processed.add(n);
                    //the following adds neighbors to the current
                    for(int neigh_nid : n.get_neighbor_ids())
                    {
                        Node neigh_n = qt.get_node_by_id(neigh_nid);
                        if(!processed.contains(neigh_n) && !processing_nodes.contains(neigh_n))
                        {
                            processing_nodes.add(neigh_n);
                        }
                    }

                    if(n.get_size() == 0)
                    {
                        continue;
                    }

                    double max_spatial_score = 1 - Node.min_dist_to_node(lat , lon , n) / Quadtree.MAX_SPATIAL_DIST;

                    if(or_keywords == null) //sugesting this is an spatial KNN query
                    {

                        //the max_score_possible is the theoretical upperbound that the tweets within this node could achieve
                        double max_score_possible = alpha * max_spatial_score + (1 - alpha);
                        if(global_topk.size() > 0)
                        {
                            //this part does not require a lock mechanism because the new global_topk.peek().get_score() can only be greater than the current value
                            if(max_score_possible <= global_topk.peek().get_score() && global_topk.size() >= k)
                            {
                                break_flag = true;
                                break;
                            }
                        }
                    }

                    else //suggesting this is an boolean knn query
                    {

                        if(global_topk.size() > 0)
                        {
                            //this part does not require a lock mechanism because the new global_topk.peek().get_score() can only be greater than the current value
                            if(max_spatial_score <= global_topk.peek().get_score() && global_topk.size() >= k)
                            {
                                break_flag = true;
                                break;
                            }
                        }
                    }
                    //System.out.println("assigning node "+ n.get_node_id());
                    threads[i] = new LocalThread(n , k , lat , lon , pos_keywords , neg_keywords , alpha , lock , topk_results , busy , i , max_spatial_score ,  global_topk , or_keywords , threshold , kryos[i]);
                    threads[i].start();

                }


                //this is the data structure that the threads write the local top-k results to, the following if is executed if some thread updates it
                if(topk_results.size() > 0)
                {
                    lock.lock();
                    while(topk_results.size() > 0)
                    {
                        PriorityQueue<ResultTweet> local_topk = topk_results.remove();
                        for(ResultTweet rt : local_topk)
                        {
                            if(global_topk.size() < k)
                            {
                                global_topk.add(rt);
                            }

                            else
                            {
                                if(rt.get_score() > global_topk.peek().get_score())
                                {
                                    global_topk.remove();
                                    global_topk.add(rt);
                                }
                            }
                        }
                    }
                    lock.unlock();
                }
            }

            if(break_flag)
            {
                //System.out.println("broken from the flag");
                break;
            }
        }

        //System.out.println("the first while break");
        while(true)
        {
            boolean flag = true;
            for (boolean b : busy) {
                if (b) {
                    flag = false;
                     //Thread.sleep(0, 1);
                    Thread.yield();
                    //System.out.println("false index " + count);
                    break;
                }

            }

            if(flag)
            {
                //System.out.println("thread yield called");
                Thread.yield();
                break;
            }
        }

        //System.out.println("the second while break");
        while(topk_results.size() > 0)
        {
            PriorityQueue<ResultTweet> local_topk = topk_results.remove();
            for(ResultTweet rt : local_topk)
            {
                if(global_topk.size() < k)
                {
                    global_topk.add(rt);
                }

                else
                {
                    if(rt.get_score() > global_topk.peek().get_score())
                    {
                        global_topk.remove();
                        global_topk.add(rt);
                    }
                }
            }
        }
        /*System.out.println("third while break");
        try
        {
            System.out.println("terminated");
            Thread.sleep(10000);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }*/

        return global_topk;

    }


    class TweetPointer
    {
        TweetWeight tw; //each TweetWeight stands for a keyword,
        int pointer_loc;
        double weight;
        HashSet<Integer> visited;
        ArrayList<HashSet[]> neg_term_weights;
        ArrayList<String[]> neg_keywords;
        Node n;
        boolean flag;
        double alpha;
        int textual_normalizer;
        Kryo kryo;
        HashMap<Integer , Integer> tweet_id_file_map;



        //TODO needs to ensure that the size of the positive keywords must be non-negative
        //this is on the string level
        public TweetPointer(TweetWeight tw , HashSet<Integer> visited , ArrayList<HashSet[]> neg_term_weights , ArrayList<String[]> neg_keywords , Node n , double alpha , int textual_normalizer ,  HashMap<Integer , Integer> tweet_id_file_map , Kryo kryo) throws IOException {
            this.tw = tw;
            this.visited = visited;
            this.pointer_loc = 0;
            this.neg_term_weights = neg_term_weights;
            this.neg_keywords = neg_keywords;
            this.n = n;
            this.textual_normalizer = textual_normalizer;
            this.tweet_id_file_map = tweet_id_file_map;
            this.kryo = kryo;
            this.flag = adjust_pointer();
            if(flag)
            {
                this.weight =  tw.get_weights(pointer_loc) / textual_normalizer * (1 - alpha);
            }
        }

        public double get_current_weight()
        {
            return tw.get_weights(pointer_loc) /textual_normalizer * (1 - alpha);
        }

        public int get_current_tid()
        {
            /*if(tw.get_tids(pointer_loc) == 2610244)
            {
                System.out.println("detected in get current tid");
            }*/
            return tw.get_tids(pointer_loc);
        }

        public boolean adjust_pointer() throws IOException
        {
            if(!visited.contains(get_current_tid()) && pass_neg_filter(neg_term_weights , get_current_tid() , neg_keywords , n ,  tweet_id_file_map , kryo))
            {
                this.weight = tw.get_weights(pointer_loc) / textual_normalizer * (1 - alpha);
                return true;
            }

            while(this.pointer_loc < tw.get_len())
            {
                this.pointer_loc++;
                if(this.pointer_loc == tw.get_len())
                {
                    break;
                }
                if(!visited.contains(get_current_tid()) && pass_neg_filter(neg_term_weights , get_current_tid() , neg_keywords , n ,  tweet_id_file_map , kryo))
                {
                    this.weight = tw.get_weights(pointer_loc) / textual_normalizer * (1 - alpha);
                    return true;
                }
            }

            return false;
        }



    }


    class LocalThread extends Thread
    {
        Node n;
        int k;
        double lat;
        double lon;
        String[] pos_keywords;
        ArrayList<String[]> neg_phrases;
        double alpha;
        ReentrantLock lock;
        Queue<PriorityQueue<ResultTweet>> topk_results;
        boolean[] busy;
        int thread_id;
        double max_spatial_score;
        String[] or_keywords;
        double threshold;
        PriorityQueue<ResultTweet> global_topk;
        HashMap<String , TweetWeightIndexHashVal> word_tweet_weight_map;
        HashMap<Integer , Integer> tweet_id_file_map;
        Kryo kryo;


        public LocalThread(Node n , int k , double lat , double lon , String[] pos_keywords , ArrayList<String[]> neg_phrases, double alpha, ReentrantLock lock, Queue<PriorityQueue<ResultTweet>> topk_results , boolean[] busy , int thread_id, double max_spatial_score , PriorityQueue<ResultTweet> global_topk , String[] or_keywords, double threshold , Kryo kryo)
        {
            this.n = n;
            this.k = k;
            this.lat = lat;
            this.lon = lon;
            this.pos_keywords = pos_keywords;
            this.neg_phrases = neg_phrases;
            this.alpha = alpha;
            this.lock = lock;
            this.topk_results = topk_results;
            this.busy = busy;
            this.thread_id = thread_id;
            this.max_spatial_score = max_spatial_score;
            this.global_topk = global_topk;
            this.or_keywords = or_keywords;
            this.threshold = threshold;
            this.kryo = kryo;
            //System.out.println("busy " + thread_id + " is set to true");
            busy[thread_id] = true;
        }

        public void run()
        {
            try
            {
                //System.out.println("incremental search called");
                PriorityQueue<ResultTweet> local_topk = incremental_search();
                //System.out.println("incremental search terminated");
                lock.lock();
                if(local_topk != null)
                {
                    topk_results.add(local_topk);
                }
                lock.unlock();
                busy[thread_id] = false;
                //System.out.println("busy " + thread_id + " is set to false");
            }

            catch (Exception e)
            {
                e.printStackTrace();
            }
        }


        public TweetWeightIndex[] get_pos_tweet_weight_index() {
            TweetWeightIndex[] pos_twis = new TweetWeightIndex[pos_keywords.length];

            for(int i = 0 ; i < pos_twis.length ; i++)
            {
                if(word_tweet_weight_map.get(pos_keywords[i]) == null)
                {
                    pos_twis[i] = null;
                }
                else
                {
                    pos_twis[i] = new TweetWeightIndex(pos_keywords[i] , word_tweet_weight_map.get(pos_keywords[i]).get_indicator() , word_tweet_weight_map.get(pos_keywords[i]).get_max_weight());
                }
            }

            return pos_twis;
        }

        public SpatialBufferEntry read_leaf_nodes_objs(int node_id , boolean booleanKNN) throws IOException {
            if(!booleanKNN)
            {
                long file_size = new File(FlushToDisk.TREE + "/" + node_id + "/SpatialMap.bin").length();
                Input input = new Input(new FileInputStream(FlushToDisk.TREE + "/" + node_id + "/SpatialMap.bin" ));
                //System.out.println(FlushToDisk.TREE + "/" + node_id + "/SpatialMap.bin");
                HashMap<Integer , SpatialTweet> id_to_spatial = kryo.readObject(input , HashMap.class);
                input.close();

                file_size += new File(FlushToDisk.TREE + "/" + node_id + "/Grids.bin").length();
                input = new Input(new FileInputStream(FlushToDisk.TREE + "/" + node_id + "/Grids.bin" ));
                HashMap<Integer, Node.GridInNode> grid_map = kryo.readObject(input , HashMap.class);
                input.close();

                return new SpatialBufferEntry(id_to_spatial , grid_map, file_size);
            }

            else
            {
                long file_size = new File(FlushToDisk.TREE + "/" + node_id + "/SpatialMap.bin").length();
                Input input = new Input(new FileInputStream(FlushToDisk.TREE + "/" + node_id + "/SpatialMap.bin" ));
                HashMap<Integer , SpatialTweet> id_to_spatial = kryo.readObject(input , HashMap.class);
                input.close();
                return new SpatialBufferEntry(id_to_spatial , file_size);
            }




        }

        //TODO normalize
        //TODO need to consider case when the negative keywords do not present
        public Object[] get_pos_neg_tweet_weight(TweetWeightIndex[] pos_twis) throws IOException {

            TweetWeight[] pos_tws = new TweetWeight[pos_twis.length];

            //TODO either the pos or the neg could be none, indicating the get_word does not exists in the node

            for(int i = 0 ; i < pos_twis.length ; i++)
            {
                pos_tws[i] = ReadFromDisk.read_tweet_weights_using_index(pos_twis[i] , n.get_node_id() , kryo);
            }


            ArrayList<TweetWeightIndex[]> neg_twis_kws = new ArrayList<>();
            for (String[] neg_phrase : neg_phrases)
            {
                TweetWeightIndex[] neg_twis = new TweetWeightIndex[neg_phrase.length];

                boolean flag = true;
                for (int i = 0; i < neg_phrase.length; i++)
                {
                    if (!word_tweet_weight_map.containsKey(neg_phrase[i]))
                    {
                        //neg_twis_kws.add(null);
                        flag = false;
                        break;
                    }
                    else
                        {
                        neg_twis[i] = new TweetWeightIndex(neg_phrase[i], word_tweet_weight_map.get(neg_phrase[i]).get_indicator(), word_tweet_weight_map.get(neg_phrase[i]).get_max_weight());
                    }
                }

                if (flag) {
                    neg_twis_kws.add(neg_twis);
                }
            }

            //System.out.println(neg_twis_kws.size() + " this is the size ");
            neg_twis_kws.sort(Comparator.comparingInt(o -> o.length));

            //TweetWeight[] neg_tws = new TweetWeight[neg_twis_kws.size()];

            ArrayList<HashSet[]> neg_tws = new ArrayList<>();

            /*for(int i = 0 ; i < neg_twis_kws.size() ; i++)
            {
                TweetWeightIndex[] twi = neg_twis_kws.get(i);
                TweetWeight[] tw = new TweetWeight[twi.length];
                for(int j = 0 ; j < tw.length ; j++)
                {
                    tw[j] = ReadFromDisk.read_tweet_weights_using_index(twi[j] , n.get_node_id() , kryo);
                }
                neg_tws.add(tw);
            }*/

            for (TweetWeightIndex[] twi : neg_twis_kws) {
                HashSet<Integer>[] hs = new HashSet[twi.length];
                for (int j = 0; j < twi.length; j++) {
                    HashSet<Integer> set = ReadFromDisk.read_tweet_hashset_using_index(twi[j], n.get_node_id(), kryo);
                    hs[j] = set;
                }
                neg_tws.add(hs);
            }


            return new Object[]{pos_tws , neg_tws};
        }


        public Object[] get_hashset() throws IOException {

            for(String str : pos_keywords)
            {
                if(word_tweet_weight_map.get(str) == null)
                {
                    return null;
                }
            }

            int or_words_count = 0;
            for(String str : or_keywords)
            {
                if(word_tweet_weight_map.containsKey(str))
                {
                    or_words_count = 1;
                    break;
                }
            }

            if(or_words_count == 0)
            {
                return null;
            }


            ArrayList<String> poskws = new ArrayList<>(Arrays.asList(pos_keywords));
            poskws.sort(Comparator.comparingInt(o -> word_tweet_weight_map.get(o).getLen()));
            HashSet<Integer> overall = null;

            for(String str : pos_keywords)
            {
                HashSet<Integer> and = ReadFromDisk.read_tweet_hashset_using_index(new TweetWeightIndex(str , word_tweet_weight_map.get(str).get_indicator() , -1) , n.get_node_id() , kryo);
                if(overall == null)
                {
                    overall = and;
                }

                else
                {
                    overall.retainAll(and);
                    if(overall.size() == 0)
                    {
                        return null;
                    }
                }


            }

            //System.out.println("and passed");

            HashSet<Integer> overallor = new HashSet<>();
            for(String str : or_keywords)
            {
                if(!word_tweet_weight_map.containsKey(str))
                {
                    continue;
                }
                HashSet<Integer> or = ReadFromDisk.read_tweet_hashset_using_index(new TweetWeightIndex(str , word_tweet_weight_map.get(str).get_indicator() , -1) , n.get_node_id() , kryo);
                if(or != null && or.size() > 0)
                {
                    overallor.addAll(or);
                }
            }

            overall.retainAll(overallor);
            if(overall.size() == 0)
            {
                return null;
            }


            //System.out.println("or passed");

            ArrayList<HashMap<String , HashSet<Integer>>> set_maps = new ArrayList<>();
            for(String[] neg_phrase : neg_phrases)
            {
                HashMap<String , HashSet<Integer>> set_map = new HashMap<>();
                boolean add_flag = true;
                for(String str : neg_phrase)
                {
                    if(!word_tweet_weight_map.containsKey(str))
                    {
                        add_flag = false;
                        break;
                    }
                    HashSet<Integer> not = ReadFromDisk.read_tweet_hashset_using_index(new TweetWeightIndex(str , word_tweet_weight_map.get(str).get_indicator() , -1) , n.get_node_id() , kryo);
                    if(not != null && not.size() == 0)
                    {
                        set_map.put(str , not);
                    }
                }
                if(add_flag)
                {
                    set_maps.add(set_map);
                }

                else
                {
                    set_maps.add(new HashMap<>());
                }
            }

            set_maps.sort(Comparator.comparingInt(o -> o.keySet().size()));


            return new Object[]{overall , set_maps};



        }


        public PriorityQueue<ResultTweet> incremental_search() throws IOException
        {

            //TODO
            /*read_leaf_nodes_objs(n.get_node_id());
            word_tweet_weight_map = n.get_weight_index();
            tweet_id_file_map = n.get_textual_index();
            TweetWeightIndex[] pos_twis = get_pos_tweet_weight_index();
            double max_text_score = 0;
            for(TweetWeightIndex twi : pos_twis)
            {
                if(twi != null)
                {
                    max_text_score +=  twi.get_max_weight();
                }
            }

            if(max_text_score > 1)
            {
                max_text_score = 1;
            }

            double weighted_max_spatial_score = max_spatial_score * alpha;
            double weighted_max_text_score = max_text_score * (1 - alpha);


            if(global_topk.size() > 0)
            {
                if(weighted_max_spatial_score + weighted_max_text_score < global_topk.peek().get_score())
                {
                    //System.out.println("node " + n.get_node_id() + " returned null");
                    return null;
                }
            }



            Object[] ret = get_pos_neg_tweet_weight(pos_twis , word_tweet_weight_map);
            TweetWeight[] pos_term_weights = (TweetWeight[]) ret[0];
            TweetWeight[] neg_term_weights = (TweetWeight[]) ret[1];
            PriorityQueue<ResultTweet> local_topk = new PriorityQueue<>(Comparator.comparingDouble(ResultTweet::get_score));*/

            if(or_keywords != null) //suggesting this is an AND query
            {
                return process_booleanKNN_query();
            }

            else
            {
                return alpha <= threshold ? spatialKNN_textual_pruning() : spatialKNN_spatial_pruning();
            }



            //initialization ends at this point, the top entry from each keyword list has been added to the priority queue and the upperbound score has been initialized
        }

        private PriorityQueue<ResultTweet> spatialKNN_textual_pruning() throws IOException {
            if(alpha != 0)
            {
                lock.lock();
                boolean contains = spatial_buffer.containsKey(n.get_node_id());
                if(contains)
                {
                    SpatialBufferEntry sbe = spatial_buffer.get(n.get_node_id());
                    sbe.increment_hit();
                    n.set_id_to_spatial(sbe.get_id_to_spatial_map());
                    n.set_grid_map(sbe.get_grid_map());
                }
                else
                {

                    SpatialBufferEntry sbe = read_leaf_nodes_objs(n.get_node_id() , false);
                    n.set_id_to_spatial(sbe.get_id_to_spatial_map());
                    n.set_grid_map(sbe.get_grid_map());

                    while (current_spatial_buffer + sbe.get_size() > spatial_buffer_size)
                    {
                        int min_hit = Integer.MAX_VALUE;
                        int min_hit_nid = -1;
                        for (int nid : spatial_buffer.keySet())
                        {
                            if (spatial_buffer.get(nid).get_hit() < min_hit)
                            {
                                min_hit = spatial_buffer.get(nid).get_hit();
                                min_hit_nid = nid;
                            }
                        }

                        long size = spatial_buffer.get(min_hit_nid).get_size();
                        spatial_buffer.remove(min_hit_nid);
                        current_spatial_buffer -= size;
                    }

                    spatial_buffer.put(n.get_node_id() , sbe);
                    current_spatial_buffer += sbe.get_size();
                }
                lock.unlock();
            }

            /*if(n.get_id2spatial().containsKey(2610244))
            {
                System.out.println("the key is included , the node id is " + n.get_node_id());
            }*/

            word_tweet_weight_map = n.get_weight_index();
            tweet_id_file_map = n.get_textual_index();
            TweetWeightIndex[] pos_twis = get_pos_tweet_weight_index();
            double max_text_score = 0;
            for(TweetWeightIndex twi : pos_twis)
            {
                if(twi != null)
                {
                    max_text_score +=  twi.get_max_weight();
                }
            }

            if(max_text_score > 1)
            {
                max_text_score = 1;
            }

            double weighted_max_spatial_score = max_spatial_score * alpha;
            double weighted_max_text_score = max_text_score * (1 - alpha);


            if(global_topk.size() > 0)
            {
                if(weighted_max_spatial_score + weighted_max_text_score <= global_topk.peek().get_score() && global_topk.size() >= k)
                {
                    //System.out.println("node " + n.get_node_id() + " returned null");
                    return null;
                }
            }



            Object[] ret = get_pos_neg_tweet_weight(pos_twis);
            TweetWeight[] pos_term_weights = (TweetWeight[]) ret[0];
            ArrayList<HashSet[]> neg_term_sets = (ArrayList<HashSet[]>) ret[1];
            PriorityQueue<ResultTweet> local_topk = new PriorityQueue<>(Comparator.comparingDouble(ResultTweet::get_score));

            HashSet<Integer> visited = new HashSet<>();
            //in the following, the greater value has a higher priority.
            PriorityQueue<TweetPointer> intermediate_results = new PriorityQueue<>((o1, o2) -> Double.compare(o2.weight , o1.weight));

            //the following is the initialization for term objects

            double text_upperbound = 0;

            for(TweetWeight tw : pos_term_weights)
            {
                if(tw != null)
                {
                    TweetPointer tp = new TweetPointer(tw , visited , neg_term_sets , neg_phrases, n , alpha , pos_term_weights.length  , tweet_id_file_map , kryo);
                    //TODO the following cannot be removed
                    if(tp.flag) //suggesting that this entry has not yet come to the end
                    {
                        text_upperbound += tp.get_current_weight();
                        intermediate_results.add(tp);
                    }
                }
            }

            //TODO the following is the initialization for spatial dimension
            //when reached here the initialization is complete



            //the incremental search terminates when the lower bound is greater or equal than the upper bound
            while(intermediate_results.size() > 0)
            {
                if(local_topk.size() == k)
                {
                    break;
                }

                TweetPointer tp = intermediate_results.remove();
                //System.out.println(tp.get_current_tid() + " this is the id");
                double before_score = tp.get_current_weight();
                boolean flag = tp.adjust_pointer(); //we need to adjust the pointer here because this id might have been visited before
                if(flag)
                {
                    double after_score = tp.get_current_weight();
                    add_to_pq(local_topk , tp.get_current_tid() , pos_term_weights , alpha , k, true);
                    text_upperbound -= before_score;
                    text_upperbound += after_score;
                    visited.add(tp.get_current_tid());
                    boolean add_flag = tp.adjust_pointer();
                    if(add_flag)
                    {
                        intermediate_results.add(tp);
                    }

                }

                else
                {
                    text_upperbound -= before_score;
                }
            }

            if(local_topk.size() < k)
            {
                return local_topk;
            }



            double lower_bound = Double.MAX_VALUE;
            for(ResultTweet rt : local_topk)
            {
                if(rt.get_score() < lower_bound)
                {
                    lower_bound = rt.get_score();
                }
            }

            double max_spatial_score = alpha * (1 - Node.min_dist_to_node(lat , lon , n) / Quadtree.MAX_SPATIAL_DIST);
            HashSet<Integer> needs_visit = new HashSet<>();

            while(intermediate_results.size() > 0)
            {
                TweetPointer tp = intermediate_results.remove();
                double before_score = tp.get_current_weight();
                boolean flag = tp.adjust_pointer(); //we need to adjust the pointer here because this id might have been visited before
                if(flag)
                {
                    int tid = tp.get_current_tid();
                    double after_score = tp.get_current_weight();
                    text_upperbound -= before_score;
                    text_upperbound += after_score;

                    if(lower_bound >=text_upperbound + max_spatial_score)
                    {
                        break;
                    }


                    /*double textual_score = add_to_pq(local_topk , tid , pos_term_weights , alpha , k , false);
                    if(lower_bound >= textual_score + max_spatial_score)
                    {
                        break;
                    }*/



                    visited.add(tid);
                    needs_visit.add(tid);
                    boolean add_flag = tp.adjust_pointer();
                    if(add_flag)
                    {
                        intermediate_results.add(tp);
                    }
                }

                else
                {
                    text_upperbound -= before_score;
                }
            }

            //System.out.println("the needs visit size is " + needs_visit.size() + " node size is " + n.get_size());
            if(local_topk.size() == k)
            {
                for(int id : needs_visit)
                {
                    add_to_pq(local_topk , id , pos_term_weights , alpha , k , true);
                }
            }

            return local_topk;
        }




        private PriorityQueue<ResultTweet> spatialKNN_spatial_pruning() throws IOException {

            lock.lock();
            boolean contains = spatial_buffer.containsKey(n.get_node_id());
            if(contains)
            {
                SpatialBufferEntry sbe = spatial_buffer.get(n.get_node_id());
                sbe.increment_hit();
                n.set_id_to_spatial(sbe.get_id_to_spatial_map());
                n.set_grid_map(sbe.get_grid_map());
            }
            else
            {

                SpatialBufferEntry sbe = read_leaf_nodes_objs(n.get_node_id() , false);
                n.set_id_to_spatial(sbe.get_id_to_spatial_map());
                n.set_grid_map(sbe.get_grid_map());

                while (current_spatial_buffer + sbe.get_size() > spatial_buffer_size)
                {
                    int min_hit = Integer.MAX_VALUE;
                    int min_hit_nid = -1;
                    for (int nid : spatial_buffer.keySet())
                    {
                        if (spatial_buffer.get(nid).get_hit() < min_hit)
                        {
                            min_hit = spatial_buffer.get(nid).get_hit();
                            min_hit_nid = nid;
                        }
                    }

                    long size = spatial_buffer.get(min_hit_nid).get_size();
                    spatial_buffer.remove(min_hit_nid);
                    current_spatial_buffer -= size;
                }

                spatial_buffer.put(n.get_node_id() , sbe);
                current_spatial_buffer += sbe.get_size();
            }
            lock.unlock();
            word_tweet_weight_map = n.get_weight_index();
            tweet_id_file_map = n.get_textual_index();
            TweetWeightIndex[] pos_twis = get_pos_tweet_weight_index();
            double max_text_score = 0;
            for(TweetWeightIndex twi : pos_twis)
            {
                if(twi != null)
                {
                    max_text_score +=  twi.get_max_weight();
                }
            }

            if(max_text_score > 1)
            {
                max_text_score = 1;
            }

            double weighted_max_spatial_score = max_spatial_score * alpha;
            double weighted_max_text_score = max_text_score * (1 - alpha);


            if(global_topk.size() > 0)
            {
                if(weighted_max_spatial_score + weighted_max_text_score <= global_topk.peek().get_score() && global_topk.size() >= k)
                {
                    //System.out.println("node " + n.get_node_id() + " returned null");
                    return null;
                }
            }



            Object[] ret = get_pos_neg_tweet_weight(pos_twis);
            TweetWeight[] pos_term_weights = (TweetWeight[]) ret[0];
            ArrayList<HashSet[]> neg_term_weights = (ArrayList<HashSet[]>) ret[1];
            PriorityQueue<ResultTweet> local_topk = new PriorityQueue<>(Comparator.comparingDouble(ResultTweet::get_score));

            Object[] grids = n.get_grids();
            class QueueElement
            {
                //the grid variable is used to determine weather or not it is a grid
                Node.GridInNode grid;
                int tid;
                double spatial_score;

                public QueueElement(Node.GridInNode grid)
                {
                    this.grid = grid;
                    this.tid = -1;
                    this.spatial_score = 1 - (grid.get_min_dist(lat , lon) / Quadtree.MAX_SPATIAL_DIST);
                    //System.out.println("the spatial score of the grid is " + this.spatial_score);
                }

                public QueueElement(int tid)
                {
                    this.tid = tid;
                    this.grid = null;
                    this.spatial_score = n.get_spatial_score_by_id(tid , lat , lon , alpha);
                }

                public Node.GridInNode get_grid()
                {
                    return grid;
                }

                public int get_tid()
                {
                    return tid;
                }

                public boolean is_tweet()
                {
                    return grid == null;
                }

                public double get_spatial_score()
                {
                    return spatial_score;
                }

                public HashSet<SpatialTweet> get_objects()
                {
                    return grid.get_objs();
                }
            }


            PriorityQueue<QueueElement> pq = new PriorityQueue<>((o1, o2) -> Double.compare(o2.get_spatial_score() , o1.get_spatial_score()));

            for(Object gn : grids)
            {
                pq.add(new QueueElement((Node.GridInNode)gn));
            }

            double kplus1_score = Double.MAX_VALUE;
            //System.out.println("while in 1 node " + n.get_node_id());
            while(pq.size() > 0)
            {

                QueueElement e = pq.remove();
                //System.out.println("e is " + e.is_tweet() + "pq size is " + pq.size() + "score is " +  e.spatial_score + " local topk size " + local_topk.size());
                //int type = e.get_type();
                boolean is_tweet = e.is_tweet();
                if(is_tweet)
                {
                    if(pass_neg_filter(neg_term_weights , e.get_tid() , neg_phrases, n , tweet_id_file_map , kryo))
                    {
                        double a_score = aggregate_score(e.get_tid() , pos_term_weights , alpha , true);
                       // System.out.println("a score == min ?" + ( a_score == Double.MIN_VALUE));
                        if(a_score != Double.MIN_VALUE)
                        {
                            local_topk.add(new ResultTweet(e.get_tid() , a_score));
                            //System.out.println("ascore is " + a_score + " kplus score is " + kplus1_score);
                            if(a_score < kplus1_score)
                            {
                                kplus1_score = a_score;
                                //System.out.println("loop , now kplusone is " + kplus1_score);
                            }
                        }

                        if(local_topk.size() == k)
                        {
                            break;
                        }
                    }



                }

                else
                {
                    for(SpatialTweet st : e.get_objects())
                    {
                        pq.add(new QueueElement(st.get_oid()));
                    }
                }
            }



            HashSet<Integer> needs_visit = new HashSet<>();
            //System.out.println("while in 2 node " + n.get_node_id());
            while(pq.size() > 0)
            {

                QueueElement e = pq.remove();
                //System.out.println("second time e is " + e.is_tweet());
                if(e.is_tweet())
                {
                    int tid = e.get_tid();
                    double weighted_spatial_score = alpha * e.get_spatial_score();
                    if(weighted_spatial_score + weighted_max_text_score <= kplus1_score)
                    {
                        break;
                    }
                    needs_visit.add(tid);
                }

                else
                {
                    for(SpatialTweet st : e.get_objects())
                    {
                        pq.add(new QueueElement(st.get_oid()));
                    }
                }
            }


            if(local_topk.size() == k)
            {
                for(int id : needs_visit)
                {
                    add_to_pq(local_topk , id , pos_term_weights , alpha , k , true);
                }
            }


            return local_topk;


        }


        private PriorityQueue<ResultTweet> process_booleanKNN_query() throws IOException {
            //System.out.println("process boolean query called this is id " + n.get_node_id());
            lock.lock();
            boolean contains = spatial_buffer.containsKey(n.get_node_id());
            if (contains) {
                SpatialBufferEntry sbe = spatial_buffer.get(n.get_node_id());
                sbe.increment_hit();
                n.set_id_to_spatial(sbe.get_id_to_spatial_map());
                //n.set_grid_map(sbe.get_grid_map());
            } else {

                SpatialBufferEntry sbe = read_leaf_nodes_objs(n.get_node_id(), true);
                n.set_id_to_spatial(sbe.get_id_to_spatial_map());
                //n.set_grid_map(sbe.get_grid_map());

                while (current_spatial_buffer + sbe.get_size() > spatial_buffer_size) {
                    int min_hit = Integer.MAX_VALUE;
                    int min_hit_nid = -1;
                    for (int nid : spatial_buffer.keySet()) {
                        if (spatial_buffer.get(nid).get_hit() < min_hit) {
                            min_hit = spatial_buffer.get(nid).get_hit();
                            min_hit_nid = nid;
                        }
                    }

                    long size = spatial_buffer.get(min_hit_nid).get_size();
                    spatial_buffer.remove(min_hit_nid);
                    current_spatial_buffer -= size;
                }

                spatial_buffer.put(n.get_node_id(), sbe);
                current_spatial_buffer += sbe.get_size();
            }
            lock.unlock();

            word_tweet_weight_map = n.get_weight_index();
            tweet_id_file_map = n.get_textual_index();

            /*TweetWeightIndex[] pos_twis = get_pos_tweet_weight_index();
            for (TweetWeightIndex twi : pos_twis) {
                if (twi == null) {
                    System.out.println("!!!");
                    return null;
                }
            }*/

            if (global_topk.size() > 0) {
                if (max_spatial_score <= global_topk.peek().get_score() && global_topk.size() >= k) {
                    //System.out.println("???");
                    return null;
                }
            }



            PriorityQueue<ResultTweet> local_topk = new PriorityQueue<>(Comparator.comparingDouble(ResultTweet::get_score));

            Object[] ret = get_hashset();

            if (ret == null) {
                //System.out.println("|||");
                return local_topk;
            }

            HashSet<Integer> overall = (HashSet<Integer>) ret[0];
            ArrayList<HashMap<String , HashSet<Integer>>> setmap_list = (ArrayList<HashMap<String , HashSet<Integer>>>) ret[1];



            for (int tid : overall)
            {
                if (pass_negative_test(tid , setmap_list))
                {
                    double spatial_score = n.get_spatial_score_by_id(tid, lat, lon, alpha);
                    if (local_topk.size() < k)
                    {
                        local_topk.add(new ResultTweet(tid, spatial_score));
                    }
                    else {
                        if (spatial_score > local_topk.peek().get_score())
                        {
                            local_topk.remove();
                            local_topk.add(new ResultTweet(tid, spatial_score));
                        }
                    }
                }

            }
            return local_topk;
        }


        public boolean pass_negative_test(int tid , ArrayList<HashMap<String , HashSet<Integer>>> setmap_list) throws IOException {

            //System.out.println("setmap list size is " + setmap_list.size());
            for(int i = 0 ; i < setmap_list.size() ; i++)
            {
                HashMap<String , HashSet<Integer>> setmap = setmap_list.get(i);
                boolean pass_current_filter = false;
                for (String not : setmap.keySet())
                {
                    if (!setmap.containsKey(not) || !setmap.get(not).contains(tid))
                    {
                        pass_current_filter = true; //meaning that we pass the filter
                        break;
                    }
                }

                if(!pass_current_filter)
                {
                    if(setmap.keySet().size() == 1)
                    {
                        return false;
                    }
                    else
                    {
                        TweetTexual tt = ReadFromDisk.read_tweet_textual_using_id(tid, tweet_id_file_map.get(tid), n.get_node_id(), kryo);
                        boolean match = strings_match(tt.get_strings(), neg_phrases.get(i));
                        if(match)
                        {
                            return false;
                        }
                    }
                }
            }

            return true;
        }





        public double add_to_pq(PriorityQueue<ResultTweet> local_topk , int tid , TweetWeight[] pos_term_weights ,  double alpha , int k , boolean add_spatial)
        {
            /*if(tid == 2610244)
            {
                System.out.println("this entry considered");
            }*/
            double total_score = aggregate_score(tid , pos_term_weights , alpha , add_spatial);
            if(total_score == Double.MIN_VALUE)
            {
                return total_score;
            }

            if(add_spatial)
            {
                ResultTweet rt = new ResultTweet(tid , total_score);
                if(local_topk.size() < k )
                {
                    local_topk.add(rt);
                }

                else
                {
                    double kth = local_topk.peek().get_score();
                    if(total_score > kth)
                    {
                        local_topk.remove();
                        local_topk.add(rt);
                    }
                }
                return local_topk.peek().get_score();
            }

            else
            {
                return total_score;
            }
        }

        //TODO : need to consider the textual and spatial normalizer
        public double aggregate_score(int tid , TweetWeight[] pos_term_weights ,  double alpha, boolean add_spatial)
        {
            double t_score = 0;
            boolean flag = false;
            for(TweetWeight tw : pos_term_weights)
            {
                if(tw != null)
                {
                    if(tw.get_hash().containsKey(tid))
                    {
                        flag = true;
                        t_score += tw.get_hash().get(tid);
                    }
                }
            }

            if(!flag)
            {
                return Double.MIN_VALUE;
            }

            if(add_spatial)
            {
                double s_score = (alpha == 0 ? 0 : n.get_spatial_score_by_id(tid , lat , lon , alpha));
                return alpha * s_score + (1 - alpha) * t_score;
            }

            else
            {
                return (1 - alpha) * t_score;
            }
        }
    }



    public boolean pass_neg_filter(ArrayList<HashSet[]> neg_term_weights , int tid , ArrayList<String[]> neg_keywords , Node n ,  HashMap<Integer , Integer> tweet_id_file_map , Kryo kryo) throws IOException
    {

        for(int i = 0 ; i < neg_term_weights.size() ; i++)
        {
            HashSet[] hs = neg_term_weights.get(i);
            //in this for loop we determine whether or not one specific term is included
            boolean pass_this_round = false;

            for(HashSet tw : hs)
            {
                if(tw == null || !tw.contains(tid))
                {
                    pass_this_round = true; //meaning that we pass the filter
                    break;
                }
            }

            if(!pass_this_round)
            {
                if(hs.length == 1)
                {
                    return false;
                }

                else
                {

                    TweetTexual tt = ReadFromDisk.read_tweet_textual_using_id(tid , tweet_id_file_map.get(tid) , n.get_node_id() ,  kryo);
                    boolean match = strings_match(tt.get_strings() , neg_keywords.get(i));
                    if(match)
                    {
                        return false;
                    }
                }
            }

        }

        return true;



        //if the for loop terminates without returning, then it means that
    }


    private boolean strings_match(String[] tweet_str , String[] neg_str)
    {
        if(tweet_str.length == 0)
        {
            return false;
        }
        int i = 0;
        int j = 0;
        int[] next = getNext(neg_str);
        while (i < tweet_str.length && j < neg_str.length) {
            if (j == -1 || tweet_str[i].equals(neg_str[j]))
            {
                i++;
                j++;
            }
            else
            {
                j = next[j];
            }
        }
        return j == neg_str.length;
    }

    private int[] getNext(String[] str)
    {
        int[] next = new int[str.length];
        int k = -1;
        int j = 0;
        next[0] = -1;
        while (j < str.length - 1) {
            if (k == -1 || str[j].equals(str[k]))
            {
                k++;
                j++;
                next[j] = k;
            }
            else
            {
                k = next[k];
            }
        }
        return next;
    }


    // NEW THREAD CODE -------------------------------------------------------------------------------------------
    // NEW THREAD CODE -------------------------------------------------------------------------------------------
    // NEW THREAD CODE -------------------------------------------------------------------------------------------

    class LocalThreadRange extends Thread
    {
        Node n;
        int k;
        double lat1;
        double lat2;
        double lon1;
        double lon2;
        String[] pos_keywords;
        ArrayList<String[]> neg_phrases;
        double alpha;
        ReentrantLock lock;
        // Queue<PriorityQueue<ResultTweet>> topk_results;
        Queue<ArrayList<ResultTweet>> loc_results;
        boolean[] busy;
        int thread_id;
        double max_spatial_score;
        String[] or_keywords;
        double threshold;
        // PriorityQueue<ResultTweet> global_topk;
        ArrayList<ResultTweet> global_results;
        HashMap<String , TweetWeightIndexHashVal> word_tweet_weight_map;
        HashMap<Integer , Integer> tweet_id_file_map;
        Kryo kryo;


        public LocalThreadRange(Node n , int k , double lat1 ,double lat2, double lon1, double lon2 , String[] pos_keywords , ArrayList<String[]> neg_phrases, double alpha, ReentrantLock lock, Queue<ArrayList<ResultTweet>> loc_results , boolean[] busy , int thread_id, double max_spatial_score , ArrayList<ResultTweet> global_results , String[] or_keywords, double threshold , Kryo kryo)
        {
            this.n = n;
            this.k = k;
            this.lat1 = lat1;
            this.lon1 = lon1;
            this.lat2 = lat2;
            this.lon2 = lon2;
            this.pos_keywords = pos_keywords;
            this.neg_phrases = neg_phrases;
            this.alpha = alpha;
            this.lock = lock;
            this.loc_results = loc_results;
            this.busy = busy;
            this.thread_id = thread_id;
            this.max_spatial_score = max_spatial_score;
            // this.global_topk = global_topk;
            this.global_results = global_results;
            this.or_keywords = or_keywords;
            this.threshold = threshold;
            this.kryo = kryo;
            //System.out.println("busy " + thread_id + " is set to true");
            busy[thread_id] = true;
        }

        public void run()
        {
            try
            {
                //System.out.println("incremental search called");
                ArrayList<ResultTweet> found_results = incremental_search();
                //System.out.println("incremental search terminated");
                lock.lock();
                if(found_results != null)
                {
                    loc_results.add(found_results);
                }
                lock.unlock();
                busy[thread_id] = false;
                //System.out.println("busy " + thread_id + " is set to false");
            }

            catch (Exception e)
            {
                e.printStackTrace();
            }
        }


        // public TweetWeightIndex[] get_pos_tweet_weight_index() {
        //     TweetWeightIndex[] pos_twis = new TweetWeightIndex[pos_keywords.length];

        //     for(int i = 0 ; i < pos_twis.length ; i++)
        //     {
        //         if(word_tweet_weight_map.get(pos_keywords[i]) == null)
        //         {
        //             pos_twis[i] = null;
        //         }
        //         else
        //         {
        //             pos_twis[i] = new TweetWeightIndex(pos_keywords[i] , word_tweet_weight_map.get(pos_keywords[i]).get_indicator() , word_tweet_weight_map.get(pos_keywords[i]).get_max_weight());
        //         }
        //     }

        //     return pos_twis;
        // }

        public SpatialBufferEntry read_leaf_nodes_objs(int node_id , boolean booleanKNN) throws IOException {
            if(!booleanKNN)
            {
                long file_size = new File(FlushToDisk.TREE + "/" + node_id + "/SpatialMap.bin").length();
                Input input = new Input(new FileInputStream(FlushToDisk.TREE + "/" + node_id + "/SpatialMap.bin" ));
                //System.out.println(FlushToDisk.TREE + "/" + node_id + "/SpatialMap.bin");
                HashMap<Integer , SpatialTweet> id_to_spatial = kryo.readObject(input , HashMap.class);
                input.close();

                file_size += new File(FlushToDisk.TREE + "/" + node_id + "/Grids.bin").length();
                input = new Input(new FileInputStream(FlushToDisk.TREE + "/" + node_id + "/Grids.bin" ));
                HashMap<Integer, Node.GridInNode> grid_map = kryo.readObject(input , HashMap.class);
                input.close();

                return new SpatialBufferEntry(id_to_spatial , grid_map, file_size);
            }

            else
            {
                long file_size = new File(FlushToDisk.TREE + "/" + node_id + "/SpatialMap.bin").length();
                Input input = new Input(new FileInputStream(FlushToDisk.TREE + "/" + node_id + "/SpatialMap.bin" ));
                HashMap<Integer , SpatialTweet> id_to_spatial = kryo.readObject(input , HashMap.class);
                input.close();
                return new SpatialBufferEntry(id_to_spatial , file_size);
            }
        }

        // //TODO normalize
        // //TODO need to consider case when the negative keywords do not present
        // public Object[] get_pos_neg_tweet_weight(TweetWeightIndex[] pos_twis) throws IOException {

        //     TweetWeight[] pos_tws = new TweetWeight[pos_twis.length];

        //     //TODO either the pos or the neg could be none, indicating the get_word does not exists in the node

        //     for(int i = 0 ; i < pos_twis.length ; i++)
        //     {
        //         pos_tws[i] = ReadFromDisk.read_tweet_weights_using_index(pos_twis[i] , n.get_node_id() , kryo);
        //     }


        //     ArrayList<TweetWeightIndex[]> neg_twis_kws = new ArrayList<>();
        //     for (String[] neg_phrase : neg_phrases)
        //     {
        //         TweetWeightIndex[] neg_twis = new TweetWeightIndex[neg_phrase.length];

        //         boolean flag = true;
        //         for (int i = 0; i < neg_phrase.length; i++)
        //         {
        //             if (!word_tweet_weight_map.containsKey(neg_phrase[i]))
        //             {
        //                 //neg_twis_kws.add(null);
        //                 flag = false;
        //                 break;
        //             }
        //             else
        //                 {
        //                 neg_twis[i] = new TweetWeightIndex(neg_phrase[i], word_tweet_weight_map.get(neg_phrase[i]).get_indicator(), word_tweet_weight_map.get(neg_phrase[i]).get_max_weight());
        //             }
        //         }

        //         if (flag) {
        //             neg_twis_kws.add(neg_twis);
        //         }
        //     }

        //     //System.out.println(neg_twis_kws.size() + " this is the size ");
        //     neg_twis_kws.sort(Comparator.comparingInt(o -> o.length));

        //     //TweetWeight[] neg_tws = new TweetWeight[neg_twis_kws.size()];

        //     ArrayList<HashSet[]> neg_tws = new ArrayList<>();

        //     /*for(int i = 0 ; i < neg_twis_kws.size() ; i++)
        //     {
        //         TweetWeightIndex[] twi = neg_twis_kws.get(i);
        //         TweetWeight[] tw = new TweetWeight[twi.length];
        //         for(int j = 0 ; j < tw.length ; j++)
        //         {
        //             tw[j] = ReadFromDisk.read_tweet_weights_using_index(twi[j] , n.get_node_id() , kryo);
        //         }
        //         neg_tws.add(tw);
        //     }*/

        //     for (TweetWeightIndex[] twi : neg_twis_kws) {
        //         HashSet<Integer>[] hs = new HashSet[twi.length];
        //         for (int j = 0; j < twi.length; j++) {
        //             HashSet<Integer> set = ReadFromDisk.read_tweet_hashset_using_index(twi[j], n.get_node_id(), kryo);
        //             hs[j] = set;
        //         }
        //         neg_tws.add(hs);
        //     }


        //     return new Object[]{pos_tws , neg_tws};
        // }


        public Object[] get_hashset() throws IOException {

            for(String str : pos_keywords)
            {
                if(word_tweet_weight_map.get(str) == null)
                {
                    return null;
                }
            }

            int or_words_count = 0;
            for(String str : or_keywords)
            {
                if(word_tweet_weight_map.containsKey(str))
                {
                    or_words_count = 1;
                    break;
                }
            }

            if(or_words_count == 0)
            {
                return null;
            }


            ArrayList<String> poskws = new ArrayList<>(Arrays.asList(pos_keywords));
            poskws.sort(Comparator.comparingInt(o -> word_tweet_weight_map.get(o).getLen()));
            HashSet<Integer> overall = null;

            for(String str : pos_keywords)
            {
                HashSet<Integer> and = ReadFromDisk.read_tweet_hashset_using_index(new TweetWeightIndex(str , word_tweet_weight_map.get(str).get_indicator() , -1) , n.get_node_id() , kryo);
                if(overall == null)
                {
                    overall = and;
                }

                else
                {
                    overall.retainAll(and);
                    if(overall.size() == 0)
                    {
                        return null;
                    }
                }


            }

            //System.out.println("and passed");

            HashSet<Integer> overallor = new HashSet<>();
            for(String str : or_keywords)
            {
                if(!word_tweet_weight_map.containsKey(str))
                {
                    continue;
                }
                HashSet<Integer> or = ReadFromDisk.read_tweet_hashset_using_index(new TweetWeightIndex(str , word_tweet_weight_map.get(str).get_indicator() , -1) , n.get_node_id() , kryo);
                if(or != null && or.size() > 0)
                {
                    overallor.addAll(or);
                }
            }

            overall.retainAll(overallor);
            if(overall.size() == 0)
            {
                return null;
            }


            //System.out.println("or passed");

            ArrayList<HashMap<String , HashSet<Integer>>> set_maps = new ArrayList<>();
            for(String[] neg_phrase : neg_phrases)
            {
                HashMap<String , HashSet<Integer>> set_map = new HashMap<>();
                boolean add_flag = true;
                for(String str : neg_phrase)
                {
                    if(!word_tweet_weight_map.containsKey(str))
                    {
                        add_flag = false;
                        break;
                    }
                    HashSet<Integer> not = ReadFromDisk.read_tweet_hashset_using_index(new TweetWeightIndex(str , word_tweet_weight_map.get(str).get_indicator() , -1) , n.get_node_id() , kryo);
                    if(not != null && not.size() == 0)
                    {
                        set_map.put(str , not);
                    }
                }
                if(add_flag)
                {
                    set_maps.add(set_map);
                }

                else
                {
                    set_maps.add(new HashMap<>());
                }
            }

            set_maps.sort(Comparator.comparingInt(o -> o.keySet().size()));

            return new Object[]{overall , set_maps};
        }


        public ArrayList<ResultTweet> incremental_search() throws IOException
        {

            //TODO
            /*read_leaf_nodes_objs(n.get_node_id());
            word_tweet_weight_map = n.get_weight_index();
            tweet_id_file_map = n.get_textual_index();
            TweetWeightIndex[] pos_twis = get_pos_tweet_weight_index();
            double max_text_score = 0;
            for(TweetWeightIndex twi : pos_twis)
            {
                if(twi != null)
                {
                    max_text_score +=  twi.get_max_weight();
                }
            }

            if(max_text_score > 1)
            {
                max_text_score = 1;
            }

            double weighted_max_spatial_score = max_spatial_score * alpha;
            double weighted_max_text_score = max_text_score * (1 - alpha);


            if(global_topk.size() > 0)
            {
                if(weighted_max_spatial_score + weighted_max_text_score < global_topk.peek().get_score())
                {
                    //System.out.println("node " + n.get_node_id() + " returned null");
                    return null;
                }
            }



            Object[] ret = get_pos_neg_tweet_weight(pos_twis , word_tweet_weight_map);
            TweetWeight[] pos_term_weights = (TweetWeight[]) ret[0];
            TweetWeight[] neg_term_weights = (TweetWeight[]) ret[1];
            PriorityQueue<ResultTweet> local_topk = new PriorityQueue<>(Comparator.comparingDouble(ResultTweet::get_score));*/

            if(or_keywords != null) //suggesting this is an AND query
            {
                return process_booleanrange_query();
            }

            else
            {
                System.out.println("ERROR NOR BOOLEAN RANGE QUERY");
                return null;
                // return alpha <= threshold ? spatialKNN_textual_pruning() : spatialKNN_spatial_pruning();
            }



            //initialization ends at this point, the top entry from each keyword list has been added to the priority queue and the upperbound score has been initialized
        }

        // private ArrayList<ResultTweet> spatialKNN_textual_pruning() throws IOException {
        //     if(alpha != 0)
        //     {
        //         lock.lock();
        //         boolean contains = spatial_buffer.containsKey(n.get_node_id());
        //         if(contains)
        //         {
        //             SpatialBufferEntry sbe = spatial_buffer.get(n.get_node_id());
        //             sbe.increment_hit();
        //             n.set_id_to_spatial(sbe.get_id_to_spatial_map());
        //             n.set_grid_map(sbe.get_grid_map());
        //         }
        //         else
        //         {

        //             SpatialBufferEntry sbe = read_leaf_nodes_objs(n.get_node_id() , false);
        //             n.set_id_to_spatial(sbe.get_id_to_spatial_map());
        //             n.set_grid_map(sbe.get_grid_map());

        //             while (current_spatial_buffer + sbe.get_size() > spatial_buffer_size)
        //             {
        //                 int min_hit = Integer.MAX_VALUE;
        //                 int min_hit_nid = -1;
        //                 for (int nid : spatial_buffer.keySet())
        //                 {
        //                     if (spatial_buffer.get(nid).get_hit() < min_hit)
        //                     {
        //                         min_hit = spatial_buffer.get(nid).get_hit();
        //                         min_hit_nid = nid;
        //                     }
        //                 }

        //                 long size = spatial_buffer.get(min_hit_nid).get_size();
        //                 spatial_buffer.remove(min_hit_nid);
        //                 current_spatial_buffer -= size;
        //             }

        //             spatial_buffer.put(n.get_node_id() , sbe);
        //             current_spatial_buffer += sbe.get_size();
        //         }
        //         lock.unlock();
        //     }

        //     /*if(n.get_id2spatial().containsKey(2610244))
        //     {
        //         System.out.println("the key is included , the node id is " + n.get_node_id());
        //     }*/

        //     word_tweet_weight_map = n.get_weight_index();
        //     tweet_id_file_map = n.get_textual_index();
        //     TweetWeightIndex[] pos_twis = get_pos_tweet_weight_index();
        //     double max_text_score = 0;
        //     for(TweetWeightIndex twi : pos_twis)
        //     {
        //         if(twi != null)
        //         {
        //             max_text_score +=  twi.get_max_weight();
        //         }
        //     }

        //     if(max_text_score > 1)
        //     {
        //         max_text_score = 1;
        //     }

        //     double weighted_max_spatial_score = max_spatial_score * alpha;
        //     double weighted_max_text_score = max_text_score * (1 - alpha);


        //     if(global_topk.size() > 0)
        //     {
        //         if(weighted_max_spatial_score + weighted_max_text_score <= global_topk.peek().get_score() && global_topk.size() >= k)
        //         {
        //             //System.out.println("node " + n.get_node_id() + " returned null");
        //             return null;
        //         }
        //     }



        //     Object[] ret = get_pos_neg_tweet_weight(pos_twis);
        //     TweetWeight[] pos_term_weights = (TweetWeight[]) ret[0];
        //     ArrayList<HashSet[]> neg_term_sets = (ArrayList<HashSet[]>) ret[1];
        //     PriorityQueue<ResultTweet> local_topk = new PriorityQueue<>(Comparator.comparingDouble(ResultTweet::get_score));

        //     HashSet<Integer> visited = new HashSet<>();
        //     //in the following, the greater value has a higher priority.
        //     PriorityQueue<TweetPointer> intermediate_results = new PriorityQueue<>((o1, o2) -> Double.compare(o2.weight , o1.weight));

        //     //the following is the initialization for term objects

        //     double text_upperbound = 0;

        //     for(TweetWeight tw : pos_term_weights)
        //     {
        //         if(tw != null)
        //         {
        //             TweetPointer tp = new TweetPointer(tw , visited , neg_term_sets , neg_phrases, n , alpha , pos_term_weights.length  , tweet_id_file_map , kryo);
        //             //TODO the following cannot be removed
        //             if(tp.flag) //suggesting that this entry has not yet come to the end
        //             {
        //                 text_upperbound += tp.get_current_weight();
        //                 intermediate_results.add(tp);
        //             }
        //         }
        //     }

        //     //TODO the following is the initialization for spatial dimension
        //     //when reached here the initialization is complete



        //     //the incremental search terminates when the lower bound is greater or equal than the upper bound
        //     while(intermediate_results.size() > 0)
        //     {
        //         if(local_topk.size() == k)
        //         {
        //             break;
        //         }

        //         TweetPointer tp = intermediate_results.remove();
        //         //System.out.println(tp.get_current_tid() + " this is the id");
        //         double before_score = tp.get_current_weight();
        //         boolean flag = tp.adjust_pointer(); //we need to adjust the pointer here because this id might have been visited before
        //         if(flag)
        //         {
        //             double after_score = tp.get_current_weight();
        //             add_to_pq(local_topk , tp.get_current_tid() , pos_term_weights , alpha , k, true);
        //             text_upperbound -= before_score;
        //             text_upperbound += after_score;
        //             visited.add(tp.get_current_tid());
        //             boolean add_flag = tp.adjust_pointer();
        //             if(add_flag)
        //             {
        //                 intermediate_results.add(tp);
        //             }

        //         }

        //         else
        //         {
        //             text_upperbound -= before_score;
        //         }
        //     }

        //     if(local_topk.size() < k)
        //     {
        //         return local_topk;
        //     }



        //     double lower_bound = Double.MAX_VALUE;
        //     for(ResultTweet rt : local_topk)
        //     {
        //         if(rt.get_score() < lower_bound)
        //         {
        //             lower_bound = rt.get_score();
        //         }
        //     }

        //     double max_spatial_score = alpha * (1 - Node.min_dist_to_node(lat , lon , n) / Quadtree.MAX_SPATIAL_DIST);
        //     HashSet<Integer> needs_visit = new HashSet<>();

        //     while(intermediate_results.size() > 0)
        //     {
        //         TweetPointer tp = intermediate_results.remove();
        //         double before_score = tp.get_current_weight();
        //         boolean flag = tp.adjust_pointer(); //we need to adjust the pointer here because this id might have been visited before
        //         if(flag)
        //         {
        //             int tid = tp.get_current_tid();
        //             double after_score = tp.get_current_weight();
        //             text_upperbound -= before_score;
        //             text_upperbound += after_score;

        //             if(lower_bound >=text_upperbound + max_spatial_score)
        //             {
        //                 break;
        //             }


        //             /*double textual_score = add_to_pq(local_topk , tid , pos_term_weights , alpha , k , false);
        //             if(lower_bound >= textual_score + max_spatial_score)
        //             {
        //                 break;
        //             }*/



        //             visited.add(tid);
        //             needs_visit.add(tid);
        //             boolean add_flag = tp.adjust_pointer();
        //             if(add_flag)
        //             {
        //                 intermediate_results.add(tp);
        //             }
        //         }

        //         else
        //         {
        //             text_upperbound -= before_score;
        //         }
        //     }

        //     //System.out.println("the needs visit size is " + needs_visit.size() + " node size is " + n.get_size());
        //     if(local_topk.size() == k)
        //     {
        //         for(int id : needs_visit)
        //         {
        //             add_to_pq(local_topk , id , pos_term_weights , alpha , k , true);
        //         }
        //     }

        //     return local_topk;
        // }




        // private ArrayList<ResultTweet> spatialKNN_spatial_pruning() throws IOException {

        //     lock.lock();
        //     boolean contains = spatial_buffer.containsKey(n.get_node_id());
        //     if(contains)
        //     {
        //         SpatialBufferEntry sbe = spatial_buffer.get(n.get_node_id());
        //         sbe.increment_hit();
        //         n.set_id_to_spatial(sbe.get_id_to_spatial_map());
        //         n.set_grid_map(sbe.get_grid_map());
        //     }
        //     else
        //     {

        //         SpatialBufferEntry sbe = read_leaf_nodes_objs(n.get_node_id() , false);
        //         n.set_id_to_spatial(sbe.get_id_to_spatial_map());
        //         n.set_grid_map(sbe.get_grid_map());

        //         while (current_spatial_buffer + sbe.get_size() > spatial_buffer_size)
        //         {
        //             int min_hit = Integer.MAX_VALUE;
        //             int min_hit_nid = -1;
        //             for (int nid : spatial_buffer.keySet())
        //             {
        //                 if (spatial_buffer.get(nid).get_hit() < min_hit)
        //                 {
        //                     min_hit = spatial_buffer.get(nid).get_hit();
        //                     min_hit_nid = nid;
        //                 }
        //             }

        //             long size = spatial_buffer.get(min_hit_nid).get_size();
        //             spatial_buffer.remove(min_hit_nid);
        //             current_spatial_buffer -= size;
        //         }

        //         spatial_buffer.put(n.get_node_id() , sbe);
        //         current_spatial_buffer += sbe.get_size();
        //     }
        //     lock.unlock();
        //     word_tweet_weight_map = n.get_weight_index();
        //     tweet_id_file_map = n.get_textual_index();
        //     TweetWeightIndex[] pos_twis = get_pos_tweet_weight_index();
        //     double max_text_score = 0;
        //     for(TweetWeightIndex twi : pos_twis)
        //     {
        //         if(twi != null)
        //         {
        //             max_text_score +=  twi.get_max_weight();
        //         }
        //     }

        //     if(max_text_score > 1)
        //     {
        //         max_text_score = 1;
        //     }

        //     double weighted_max_spatial_score = max_spatial_score * alpha;
        //     double weighted_max_text_score = max_text_score * (1 - alpha);


        //     if(global_topk.size() > 0)
        //     {
        //         if(weighted_max_spatial_score + weighted_max_text_score <= global_topk.peek().get_score() && global_topk.size() >= k)
        //         {
        //             //System.out.println("node " + n.get_node_id() + " returned null");
        //             return null;
        //         }
        //     }



        //     Object[] ret = get_pos_neg_tweet_weight(pos_twis);
        //     TweetWeight[] pos_term_weights = (TweetWeight[]) ret[0];
        //     ArrayList<HashSet[]> neg_term_weights = (ArrayList<HashSet[]>) ret[1];
        //     PriorityQueue<ResultTweet> local_topk = new PriorityQueue<>(Comparator.comparingDouble(ResultTweet::get_score));

        //     Object[] grids = n.get_grids();
        //     class QueueElement
        //     {
        //         //the grid variable is used to determine weather or not it is a grid
        //         Node.GridInNode grid;
        //         int tid;
        //         double spatial_score;

        //         public QueueElement(Node.GridInNode grid)
        //         {
        //             this.grid = grid;
        //             this.tid = -1;
        //             this.spatial_score = 1 - (grid.get_min_dist(lat , lon) / Quadtree.MAX_SPATIAL_DIST);
        //             //System.out.println("the spatial score of the grid is " + this.spatial_score);
        //         }

        //         public QueueElement(int tid)
        //         {
        //             this.tid = tid;
        //             this.grid = null;
        //             this.spatial_score = n.get_spatial_score_by_id(tid , lat , lon , alpha);
        //         }

        //         public Node.GridInNode get_grid()
        //         {
        //             return grid;
        //         }

        //         public int get_tid()
        //         {
        //             return tid;
        //         }

        //         public boolean is_tweet()
        //         {
        //             return grid == null;
        //         }

        //         public double get_spatial_score()
        //         {
        //             return spatial_score;
        //         }

        //         public HashSet<SpatialTweet> get_objects()
        //         {
        //             return grid.get_objs();
        //         }
        //     }


        //     PriorityQueue<QueueElement> pq = new PriorityQueue<>((o1, o2) -> Double.compare(o2.get_spatial_score() , o1.get_spatial_score()));

        //     for(Object gn : grids)
        //     {
        //         pq.add(new QueueElement((Node.GridInNode)gn));
        //     }

        //     double kplus1_score = Double.MAX_VALUE;
        //     //System.out.println("while in 1 node " + n.get_node_id());
        //     while(pq.size() > 0)
        //     {

        //         QueueElement e = pq.remove();
        //         //System.out.println("e is " + e.is_tweet() + "pq size is " + pq.size() + "score is " +  e.spatial_score + " local topk size " + local_topk.size());
        //         //int type = e.get_type();
        //         boolean is_tweet = e.is_tweet();
        //         if(is_tweet)
        //         {
        //             if(pass_neg_filter(neg_term_weights , e.get_tid() , neg_phrases, n , tweet_id_file_map , kryo))
        //             {
        //                 double a_score = aggregate_score(e.get_tid() , pos_term_weights , alpha , true);
        //                // System.out.println("a score == min ?" + ( a_score == Double.MIN_VALUE));
        //                 if(a_score != Double.MIN_VALUE)
        //                 {
        //                     local_topk.add(new ResultTweet(e.get_tid() , a_score));
        //                     //System.out.println("ascore is " + a_score + " kplus score is " + kplus1_score);
        //                     if(a_score < kplus1_score)
        //                     {
        //                         kplus1_score = a_score;
        //                         //System.out.println("loop , now kplusone is " + kplus1_score);
        //                     }
        //                 }

        //                 if(local_topk.size() == k)
        //                 {
        //                     break;
        //                 }
        //             }



        //         }

        //         else
        //         {
        //             for(SpatialTweet st : e.get_objects())
        //             {
        //                 pq.add(new QueueElement(st.get_oid()));
        //             }
        //         }
        //     }



        //     HashSet<Integer> needs_visit = new HashSet<>();
        //     //System.out.println("while in 2 node " + n.get_node_id());
        //     while(pq.size() > 0)
        //     {

        //         QueueElement e = pq.remove();
        //         //System.out.println("second time e is " + e.is_tweet());
        //         if(e.is_tweet())
        //         {
        //             int tid = e.get_tid();
        //             double weighted_spatial_score = alpha * e.get_spatial_score();
        //             if(weighted_spatial_score + weighted_max_text_score <= kplus1_score)
        //             {
        //                 break;
        //             }
        //             needs_visit.add(tid);
        //         }

        //         else
        //         {
        //             for(SpatialTweet st : e.get_objects())
        //             {
        //                 pq.add(new QueueElement(st.get_oid()));
        //             }
        //         }
        //     }


        //     if(local_topk.size() == k)
        //     {
        //         for(int id : needs_visit)
        //         {
        //             add_to_pq(local_topk , id , pos_term_weights , alpha , k , true);
        //         }
        //     }


        //     return local_topk;


        // }


        private ArrayList<ResultTweet> process_booleanrange_query() throws IOException {
            //System.out.println("process boolean query called this is id " + n.get_node_id());
            lock.lock();
            boolean contains = spatial_buffer.containsKey(n.get_node_id());
            if (contains) {
                SpatialBufferEntry sbe = spatial_buffer.get(n.get_node_id());
                sbe.increment_hit();
                n.set_id_to_spatial(sbe.get_id_to_spatial_map());
                //n.set_grid_map(sbe.get_grid_map());
            } else {

                SpatialBufferEntry sbe = read_leaf_nodes_objs(n.get_node_id(), true);
                n.set_id_to_spatial(sbe.get_id_to_spatial_map());
                //n.set_grid_map(sbe.get_grid_map());

                while (current_spatial_buffer + sbe.get_size() > spatial_buffer_size) {
                    int min_hit = Integer.MAX_VALUE;
                    int min_hit_nid = -1;
                    for (int nid : spatial_buffer.keySet()) {
                        if (spatial_buffer.get(nid).get_hit() < min_hit) {
                            min_hit = spatial_buffer.get(nid).get_hit();
                            min_hit_nid = nid;
                        }
                    }

                    long size = spatial_buffer.get(min_hit_nid).get_size();
                    spatial_buffer.remove(min_hit_nid);
                    current_spatial_buffer -= size;
                }

                spatial_buffer.put(n.get_node_id(), sbe);
                current_spatial_buffer += sbe.get_size();
            }
            lock.unlock();

            word_tweet_weight_map = n.get_weight_index();
            tweet_id_file_map = n.get_textual_index();

            /*TweetWeightIndex[] pos_twis = get_pos_tweet_weight_index();
            for (TweetWeightIndex twi : pos_twis) {
                if (twi == null) {
                    System.out.println("!!!");
                    return null;
                }
            }*/

            // if (global_topk.size() > 0) {
            //     if (max_spatial_score <= global_topk.peek().get_score() && global_topk.size() >= k) {
            //         //System.out.println("???");
            //         return null;
            //     }
            // }



            // PriorityQueue<ResultTweet> local_topk = new PriorityQueue<>(Comparator.comparingDouble(ResultTweet::get_score));
            ArrayList<ResultTweet> local_result = new ArrayList<>();
            
            // check contents of node to see if any fit the keywords 
            Object[] ret = get_hashset();

            // if no item inside complies with keywords, return empty array
            if (ret == null) {
                //System.out.println("|||");
                return local_result;
            }

            HashSet<Integer> overall = (HashSet<Integer>) ret[0];
            ArrayList<HashMap<String , HashSet<Integer>>> setmap_list = (ArrayList<HashMap<String , HashSet<Integer>>>) ret[1];



            for (int tid : overall)
            {
                if (pass_negative_test(tid , setmap_list))
                {
                    // double spatial_score = n.get_spatial_score_by_id(tid, lat, lon, alpha);
                    // if (local_topk.size() < k)
                    // {
                    //     local_topk.add(new ResultTweet(tid, spatial_score));
                    // }
                    // else {
                    //     if (spatial_score > local_topk.peek().get_score())
                    //     {
                    //         local_topk.remove();
                    //         local_topk.add(new ResultTweet(tid, spatial_score));
                    //     }
                    // }
                    if (n.checkRange(tid, lat1,lat2,lon1,lon2)){
                        local_result.add(new ResultTweet(tid, 0));
                    }

                }

            }
            return local_result;
        }


        public boolean pass_negative_test(int tid , ArrayList<HashMap<String , HashSet<Integer>>> setmap_list) throws IOException {

            //System.out.println("setmap list size is " + setmap_list.size());
            for(int i = 0 ; i < setmap_list.size() ; i++)
            {
                HashMap<String , HashSet<Integer>> setmap = setmap_list.get(i);
                boolean pass_current_filter = false;
                for (String not : setmap.keySet())
                {
                    if (!setmap.containsKey(not) || !setmap.get(not).contains(tid))
                    {
                        pass_current_filter = true; //meaning that we pass the filter
                        break;
                    }
                }

                if(!pass_current_filter)
                {
                    if(setmap.keySet().size() == 1)
                    {
                        return false;
                    }
                    else
                    {
                        TweetTexual tt = ReadFromDisk.read_tweet_textual_using_id(tid, tweet_id_file_map.get(tid), n.get_node_id(), kryo);
                        boolean match = strings_match(tt.get_strings(), neg_phrases.get(i));
                        if(match)
                        {
                            return false;
                        }
                    }
                }
            }

            return true;
        }





        // public double add_to_pq(PriorityQueue<ResultTweet> local_topk , int tid , TweetWeight[] pos_term_weights ,  double alpha , int k , boolean add_spatial)
        // {
        //     /*if(tid == 2610244)
        //     {
        //         System.out.println("this entry considered");
        //     }*/
        //     double total_score = aggregate_score(tid , pos_term_weights , alpha , add_spatial);
        //     if(total_score == Double.MIN_VALUE)
        //     {
        //         return total_score;
        //     }

        //     if(add_spatial)
        //     {
        //         ResultTweet rt = new ResultTweet(tid , total_score);
        //         if(local_topk.size() < k )
        //         {
        //             local_topk.add(rt);
        //         }

        //         else
        //         {
        //             double kth = local_topk.peek().get_score();
        //             if(total_score > kth)
        //             {
        //                 local_topk.remove();
        //                 local_topk.add(rt);
        //             }
        //         }
        //         return local_topk.peek().get_score();
        //     }

        //     else
        //     {
        //         return total_score;
        //     }
        // }

        //TODO : need to consider the textual and spatial normalizer
    //     public double aggregate_score(int tid , TweetWeight[] pos_term_weights ,  double alpha, boolean add_spatial)
    //     {
    //         double t_score = 0;
    //         boolean flag = false;
    //         for(TweetWeight tw : pos_term_weights)
    //         {
    //             if(tw != null)
    //             {
    //                 if(tw.get_hash().containsKey(tid))
    //                 {
    //                     flag = true;
    //                     t_score += tw.get_hash().get(tid);
    //                 }
    //             }
    //         }

    //         if(!flag)
    //         {
    //             return Double.MIN_VALUE;
    //         }

    //         if(add_spatial)
    //         {
    //             double s_score = (alpha == 0 ? 0 : n.get_spatial_score_by_id(tid , lat , lon , alpha));
    //             return alpha * s_score + (1 - alpha) * t_score;
    //         }

    //         else
    //         {
    //             return (1 - alpha) * t_score;
    //         }
    //     }



    }

}
