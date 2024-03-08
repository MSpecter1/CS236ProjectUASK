package STOPKU.Query;



import ILQ.Tweet;
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

public class NaiveQueryProcessor {

    private Quadtree qt;
    private int num_thread;
    private long spatial_buffer_size;
    private Hashtable<Integer, SpatialBufferEntry> spatial_buffer;
    private long current_spatial_buffer;
    private Kryo[] kryos;


    public NaiveQueryProcessor(Quadtree qt, int num_thread, long spatial_buffer_size) {
        this.qt = qt;
        this.num_thread = num_thread;
        this.spatial_buffer_size = spatial_buffer_size;

        spatial_buffer = new Hashtable<>();
        current_spatial_buffer = 0;
        kryos = new Kryo[num_thread];
        for (int i = 0; i < num_thread; i++) {
            kryos[i] = new Kryo();
            Tools.init(kryos[i]);
        }


    }

    public PriorityQueue<ResultTweet> process_query(QueryInstance qi) {
        double lon = qi.get_lon();
        double lat = qi.get_lat();
        String[] pos_keywords = qi.get_pos_keywords();
        ArrayList<String[]> neg_keywords = qi.get_neg_keywords();
        int k = qi.get_k();
        double alpha = qi.get_alpha();


        HashSet<Node> processed = new HashSet<>();
        PriorityQueue<Node> processing_nodes = new PriorityQueue<>(Comparator.comparingDouble(o -> Node.min_dist_to_node(qi.get_lat(), qi.get_lon(), o)));
        processing_nodes.add(qt.search_path_to_leaf(new SpatialTweet(-1, lat, lon)));
        PriorityQueue<ResultTweet> global_topk = new PriorityQueue<>(Comparator.comparingDouble(ResultTweet::get_score));


        LocalThread[] threads = new LocalThread[num_thread];

        boolean[] busy = new boolean[num_thread];
        Queue<PriorityQueue<ResultTweet>> topk_results = new LinkedList<>();
        ReentrantLock lock = new ReentrantLock();


        boolean break_flag = false;
        while (true) {
            for (int i = 0; i < num_thread; i++) {
                if (processing_nodes.size() == 0) {
                    break_flag = true;
                    break;
                }

                if (!busy[i]) {
                    Node n = processing_nodes.remove();
                    processed.add(n);


                    //the following adds neighbors to the current
                    for (int neigh_nid : n.get_neighbor_ids()) {
                        Node neigh_n = qt.get_node_by_id(neigh_nid);
                        if (!processed.contains(neigh_n) && !processing_nodes.contains(neigh_n)) {
                            processing_nodes.add(neigh_n);
                        }
                    }

                    if (n.get_size() == 0) {
                        continue;
                    }


                    double max_spatial_score = 1 - Node.min_dist_to_node(lat, lon, n) / Quadtree.MAX_SPATIAL_DIST;
                    //the max_score_possible is the theoretical upperbound that the tweets within this node could achieve
                    double max_score_possible = alpha * max_spatial_score + (1 - alpha);


                    if (global_topk.size() > 0) {
                        //this part does not require a lock mechanism because the new global_topk.peek().get_score() can only be greater than the current value
                        if (max_score_possible <= global_topk.peek().get_score() && global_topk.size() >= k) {
                            break_flag = true;
                            break;
                        }
                    }


                    threads[i] = new LocalThread(n, k, lat, lon, pos_keywords, neg_keywords, alpha, lock, topk_results, busy, i, max_spatial_score, global_topk, kryos[i]);
                    threads[i].start();

                }


                //this is the data structure that the threads write the local top-k results to, the following if is executed if some thread updates it
                if (topk_results.size() > 0) {
                    lock.lock();
                    while (topk_results.size() > 0) {
                        PriorityQueue<ResultTweet> local_topk = topk_results.remove();
                        for (ResultTweet rt : local_topk) {
                            if (global_topk.size() < k) {
                                global_topk.add(rt);
                            } else {
                                if (rt.get_score() > global_topk.peek().get_score()) {
                                    global_topk.remove();
                                    global_topk.add(rt);
                                }
                            }
                        }
                    }
                    lock.unlock();
                }
            }

            if (break_flag) {
                break;
            }
        }


        while (true) {
            boolean flag = true;
            for (int i = 0; i < busy.length; i++) {
                if (busy[i]) {
                    flag = false;
                }
            }

            if (flag) {
                break;
            }
        }

        while (topk_results.size() > 0) {
            PriorityQueue<ResultTweet> local_topk = topk_results.remove();
            for (ResultTweet rt : local_topk) {
                if (global_topk.size() < k) {
                    global_topk.add(rt);
                } else {
                    if (rt.get_score() > global_topk.peek().get_score()) {
                        global_topk.remove();
                        global_topk.add(rt);
                    }
                }
            }
        }


        return global_topk;

    }


    class TweetPointer {
        PriorityQueue<SpatialTweetTemp> spatial_ordered;
        TweetWeight tw; //each TweetWeight stands for a keyword,
        int pointer_loc;
        double weight;
        HashSet<Integer> visited;
        ArrayList<HashSet[]> neg_term_weights;
        ArrayList<String[]> neg_phrases;
        Node n;
        boolean term;
        double alpha;
        boolean flag;
        int textual_normalizer;
        Kryo kryo;
        HashMap<Integer, Integer> tweet_id_file_map;
        TweetWeight[] pos_term_weights;


        //TODO needs to ensure that the size of the positive keywords must be non-negative
        //this is on the string level
        public TweetPointer(boolean term, TweetWeight tw, HashSet<Integer> visited, ArrayList<HashSet[]> neg_term_weights, ArrayList<String[]> neg_phrases, Node n, double alpha, int textual_normalizer, HashMap<Integer, Integer> tweet_id_file_map, Kryo kryo) throws IOException {
            this.term = term;
            this.tw = tw;
            this.visited = visited;
            this.pointer_loc = 0;
            this.neg_term_weights = neg_term_weights;
            this.neg_phrases = neg_phrases;
            this.n = n;
            this.textual_normalizer = textual_normalizer;
            this.tweet_id_file_map = tweet_id_file_map;
            this.kryo = kryo;
            this.flag = adjust_pointer();
            if (flag) {
                this.weight = tw.get_weights(pointer_loc) * (1 - alpha) / textual_normalizer;
            }
        }

        public TweetPointer(boolean term, PriorityQueue<SpatialTweetTemp> spatial_ordered, HashSet<Integer> visited, ArrayList<HashSet[]> neg_term_weights, ArrayList<String[]> neg_phrases, Node n, double alpha, HashMap<Integer, Integer> tweet_id_file_map, Kryo kryo, TweetWeight[] pos_term_weights) throws IOException {
            this.term = term;
            this.spatial_ordered = spatial_ordered;
            this.visited = visited;
            this.neg_phrases = neg_phrases;
            this.neg_term_weights = neg_term_weights;
            this.n = n;
            this.tweet_id_file_map = tweet_id_file_map;
            this.kryo = kryo;
            this.alpha = alpha;
            this.pos_term_weights = pos_term_weights;
            this.flag = adjust_pointer();
            if (flag) {
                //this.weight = spatial_ordered.peek().get_val() / alpha * (1 - alpha) /textual_normalizer;
                this.weight = spatial_ordered.peek().get_val() * alpha;
            }

        }

        public int get_current_tid() {
            if (term) {
                return tw.get_tids(pointer_loc);
            } else {
                return spatial_ordered.peek().get_tid();
            }
        }

        public boolean adjust_pointer() throws IOException {
            if (term)
            {
                if (!visited.contains(get_current_tid()) && pass_neg_filter())
                {
                    /*if(get_current_tid() == 3994530)
                    {
                        System.out.println("error1 detected");
                    }*/
                    this.weight = tw.get_weights(pointer_loc) / textual_normalizer * (1 - alpha);
                    return true;
                }

                while (this.pointer_loc < tw.get_len())
                {
                    this.pointer_loc++;
                    if (this.pointer_loc == tw.get_len())
                    {
                        break;
                    }
                    if (!visited.contains(get_current_tid()) && pass_neg_filter())
                    {
                        /*if(get_current_tid() == 3994530)
                        {
                            System.out.println("error2 detected");
                        }*/
                        this.weight = tw.get_weights(pointer_loc) / textual_normalizer * (1 - alpha);
                        return true;
                    }
                }
            }
            else
                {
                if (spatial_ordered.size() == 0) {
                    return false;
                }


                if (!visited.contains(get_current_tid()) && has_one_word() && pass_neg_filter()) {

                    this.weight = spatial_ordered.peek().get_val() * alpha;
                    return true;
                }

                while (spatial_ordered.size() > 0) {
                    spatial_ordered.remove();
                    if (spatial_ordered.size() == 0) {
                        break;
                    }
                    if (!visited.contains(get_current_tid()) &&has_one_word()&& pass_neg_filter()) {
                        /*if(get_current_tid() == 3994530)
                        {
                            System.out.println("error3 detected");
                        }*/
                        this.weight = spatial_ordered.peek().get_val() * alpha;
                        return true;
                    }
                }
            }

            this.weight = 0;
            return false;

        }

        public boolean has_one_word() {
            for (TweetWeight tw : pos_term_weights) {
                if(tw!=null)
                {
                    if (tw.get_hash().containsKey(get_current_tid())) {
                        return true;
                    }
                }
            }

            return false;
        }

        public boolean pass_neg_filter() throws IOException {

            for (int i = 0; i < neg_term_weights.size(); i++) {
                HashSet[] hs = neg_term_weights.get(i);
                //in this for loop we determine whether or not one specific term is included
                boolean pass_this_round = false;

                for (HashSet tw : hs) {
                    if (tw == null || !tw.contains(get_current_tid())) {
                        pass_this_round = true; //meaning that we pass the filter
                        break;
                    }
                }

                if (!pass_this_round) {
                    if (hs.length == 1) {
                        return false;
                    } else {
                        TweetTexual tt = ReadFromDisk.read_tweet_textual_using_id(get_current_tid(), tweet_id_file_map.get(get_current_tid()), n.get_node_id(), kryo);
                        boolean match = strings_match(tt.get_strings(), neg_phrases.get(i));
                        if (match) {
                            return false;
                        }
                    }
                }

            }

            return true;


            /*for(HashSet tw : neg_term_weights)
            {
                if(tw == null || !tw.contains(get_current_tid()))
                {
                    return true; //meaning that we pass the filter
                }
            }

            if(neg_phrases.length == 1)
            {
                return false;
            }

            TweetTexual tt = ReadFromDisk.read_tweet_textual_using_id(get_current_tid() , tweet_id_file_map.get(get_current_tid()) , n.get_node_id() ,  kryo);
            return !strings_match(tt.get_strings() , neg_phrases);
            //if the for loop terminates without returning, then it means that*/
        }

    }


    class LocalThread extends Thread {
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
        PriorityQueue<ResultTweet> global_topk;
        HashMap<String, TweetWeightIndexHashVal> word_tweet_weight_map;
        HashMap<Integer, Integer> tweet_id_file_map;
        Kryo kryo;

        public LocalThread(Node n, int k, double lat, double lon, String[] pos_keywords, ArrayList<String[]> neg_phrases, double alpha, ReentrantLock lock, Queue<PriorityQueue<ResultTweet>> topk_results, boolean[] busy, int thread_id, double max_spatial_score, PriorityQueue<ResultTweet> global_topk, Kryo kryo) {

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
            this.tweet_id_file_map = n.get_textual_index();
            this.kryo = kryo;
            busy[thread_id] = true;
        }

        public void run() {
            try {

                PriorityQueue<ResultTweet> local_topk = incremental_search();
                lock.lock();
                if (local_topk != null) {
                    topk_results.add(local_topk);
                }
                lock.unlock();
                busy[thread_id] = false;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }


        public TweetWeightIndex[] get_pos_tweet_weight_index() {
            TweetWeightIndex[] pos_twis = new TweetWeightIndex[pos_keywords.length];

            for (int i = 0; i < pos_twis.length; i++) {
                if (word_tweet_weight_map.get(pos_keywords[i]) == null) {
                    pos_twis[i] = null;
                } else {
                    pos_twis[i] = new TweetWeightIndex(pos_keywords[i], word_tweet_weight_map.get(pos_keywords[i]).get_indicator(), word_tweet_weight_map.get(pos_keywords[i]).get_max_weight());
                }
            }

            return pos_twis;
        }


        public SpatialBufferEntry read_leaf_nodes_objs(int node_id) throws IOException {
            long file_size = new File(FlushToDisk.TREE + "/" + node_id + "/SpatialMap.bin").length();
            Input input = new Input(new FileInputStream(FlushToDisk.TREE + "/" + node_id + "/SpatialMap.bin"));
            //System.out.println(FlushToDisk.TREE + "/" + node_id + "/SpatialMap.bin");
            HashMap<Integer, SpatialTweet> id_to_spatial = kryo.readObject(input, HashMap.class);
            input.close();

            file_size += new File(FlushToDisk.TREE + "/" + node_id + "/Grids.bin").length();
            input = new Input(new FileInputStream(FlushToDisk.TREE + "/" + node_id + "/Grids.bin"));
            HashMap<Integer, Node.GridInNode> grid_map = kryo.readObject(input, HashMap.class);
            input.close();

            return new SpatialBufferEntry(id_to_spatial, grid_map, file_size);

        }

        //TODO normalize
        //TODO need to consider case when the negative keywords do not present
        public Object[] get_pos_neg_tweet_weight(TweetWeightIndex[] pos_twis) throws IOException {

            TweetWeight[] pos_tws = new TweetWeight[pos_twis.length];

            //TODO either the pos or the neg could be none, indicating the get_word does not exists in the node

            for (int i = 0; i < pos_twis.length; i++) {
                pos_tws[i] = ReadFromDisk.read_tweet_weights_using_index(pos_twis[i], n.get_node_id(), kryo);
            }


            ArrayList<TweetWeightIndex[]> neg_twis_kws = new ArrayList<>();
            for (String[] neg_phrase : neg_phrases) {
                TweetWeightIndex[] neg_twis = new TweetWeightIndex[neg_phrase.length];

                boolean flag = true;
                for (int i = 0; i < neg_phrase.length; i++) {
                    if (!word_tweet_weight_map.containsKey(neg_phrase[i])) {
                        //neg_twis_kws.add(null);
                        flag = false;
                        break;
                    } else {
                        neg_twis[i] = new TweetWeightIndex(neg_phrase[i], word_tweet_weight_map.get(neg_phrase[i]).get_indicator(), word_tweet_weight_map.get(neg_phrase[i]).get_max_weight());
                    }
                }

                if (flag) {
                    neg_twis_kws.add(neg_twis);
                }
            }

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


            return new Object[]{pos_tws, neg_tws};
        }

        public Object[] sort_spatial(Node n, double lat, double lon) {
            PriorityQueue<SpatialTweetTemp> spatial_pq = new PriorityQueue<>((o1, o2) -> Double.compare(o2.get_val(), o1.get_val()));
            HashMap<Integer, Double> spatial_hash = new HashMap<>();
            HashMap<Integer, SpatialTweet> id2spatial = n.get_id2spatial();
            for (int id : id2spatial.keySet()) {
                SpatialTweet st = id2spatial.get(id);
                //since the final score is computed as alpha * SpatialProximity + (1 - alpha) * TextualProximity, to enable them on a same scale, we make
                //all the spatial distance divide by alpha and then times (1 - alpha)
                double dist = Node.compute_dist(st.get_lat(), st.get_lon(), lat, lon);
                double score = 1 - dist / Quadtree.MAX_SPATIAL_DIST;
                SpatialTweetTemp stt = new SpatialTweetTemp(st.get_oid(), score);
                spatial_pq.add(stt);
                spatial_hash.put(st.get_oid(), score);
            }


            return new Object[]{spatial_pq, spatial_hash};
        }

        public PriorityQueue<ResultTweet> incremental_search() throws IOException {
            lock.lock();
            boolean contains = spatial_buffer.containsKey(n.get_node_id());
            if (contains) {
                SpatialBufferEntry sbe = spatial_buffer.get(n.get_node_id());
                sbe.increment_hit();
                n.set_id_to_spatial(sbe.get_id_to_spatial_map());
                n.set_grid_map(sbe.get_grid_map());
            } else {
                SpatialBufferEntry sbe = read_leaf_nodes_objs(n.get_node_id());
                n.set_id_to_spatial(sbe.get_id_to_spatial_map());
                n.set_grid_map(sbe.get_grid_map());

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

            TweetWeightIndex[] pos_twis = get_pos_tweet_weight_index();


            double max_text_score = 0;
            for (TweetWeightIndex twi : pos_twis) {
                if (twi != null) {
                    max_text_score += twi.get_max_weight() / pos_twis.length;
                }
            }

            if (global_topk.size() > 0) {
                if (max_spatial_score * alpha + max_text_score * (1 - alpha) <= global_topk.peek().get_score() && global_topk.size() >= k) {
                    return null;
                }
            }

            Object[] spatial = sort_spatial(n, lat, lon);
            PriorityQueue<SpatialTweetTemp> spatial_ordered = (PriorityQueue<SpatialTweetTemp>) spatial[0];
            HashMap<Integer, Double> spatial_hash = (HashMap<Integer, Double>) spatial[1];

            //System.out.println("spatial ordered size is " + spatial_ordered.size());


            Object[] ret = get_pos_neg_tweet_weight(pos_twis);
            TweetWeight[] pos_term_weights = (TweetWeight[]) ret[0];
            ArrayList<HashSet[]> neg_term_sets = (ArrayList<HashSet[]>) ret[1];
            PriorityQueue<ResultTweet> local_topk = new PriorityQueue<>(Comparator.comparingDouble(ResultTweet::get_score));


            HashSet<Integer> visited = new HashSet<>();
            //in the following, the greater value has a higher priority.
            PriorityQueue<TweetPointer> intermediate_results = new PriorityQueue<>((o1, o2) -> Double.compare(o2.weight, o1.weight));

            double upper_bound = 0;

            //the following is the initialization for term objects
            for (TweetWeight tw : pos_term_weights) {
                if (tw != null) {
                    TweetPointer tp = new TweetPointer(true, tw, visited, neg_term_sets, neg_phrases, n, alpha, pos_term_weights.length, tweet_id_file_map, kryo);
                    if (tp.flag) //suggesting that this entry has not yet come to the end
                    {
                        intermediate_results.add(tp);

                        add_to_pq(local_topk, tp.get_current_tid(), pos_term_weights, spatial_hash, alpha, k);
                        visited.add(tp.get_current_tid());
                        //tp.adjust_pointer();
                        upper_bound += tp.weight;
                        //needs to update the upperbound when the next entry is added
                    }
                }

            }

            //the following is the initialization for spatial dimension
            TweetPointer stp = new TweetPointer(false, spatial_ordered, visited, neg_term_sets, neg_phrases, n, alpha, tweet_id_file_map, kryo, pos_term_weights);

            if (stp.flag) {
                intermediate_results.add(stp);

                add_to_pq(local_topk, stp.get_current_tid(), pos_term_weights, spatial_hash, alpha, k);
                visited.add(stp.get_current_tid());
                //stp.adjust_pointer();
                upper_bound += stp.weight;
            }

            if (local_topk.size() == 0) {
                return local_topk;
            }

            double lower_bound = local_topk.peek().get_score();
            //when reached here the initialization is complete


            //the incremental search terminates when the lower bound is greater or equal than the upper bound
            while (lower_bound <= upper_bound && intermediate_results.size() > 0) {
                if (lower_bound == upper_bound && local_topk.size() == k) {
                    break;
                }
                TweetPointer tp = intermediate_results.remove();
                double val = tp.weight;
                boolean flag = tp.adjust_pointer(); //we need to adjust the pointer here because this id might have been visited before
                if (flag) {
                    if (tp.get_current_tid() == 3995747) {
                        System.out.println(tp.spatial_ordered == null);
                        System.out.println("3995747 added");
                    }
                    lower_bound = add_to_pq(local_topk, tp.get_current_tid(), pos_term_weights, spatial_hash, alpha, k);
                    visited.add(tp.get_current_tid());
                    boolean add_flag = tp.adjust_pointer();
                    if (add_flag) {
                        double new_bound = tp.weight;
                        upper_bound = upper_bound - val + new_bound;
                        intermediate_results.add(tp);
                    } else {
                        upper_bound -= val;
                    }
                }


            }


            if (local_topk.size() < k && intermediate_results.size() > 0 && spatial_ordered.size() > 0) {
                TweetPointer tp = intermediate_results.remove();
                //System.out.println(spatial_ordered.size() + " spatial size ");
                //System.out.println(tp.get_current_tid() + "current tid");
                while (tp.adjust_pointer()) {
                    add_to_pq(local_topk, tp.get_current_tid(), pos_term_weights, spatial_hash, alpha, k);
                    visited.add(tp.get_current_tid());
                    if (local_topk.size() == k) {
                        break;
                    }
                }
            }

            return local_topk;
            //initialization ends at this point, the top entry from each keyword list has been added to the priority queue and the upperbound score has been initialized
        }


        public double add_to_pq(PriorityQueue<ResultTweet> local_topk, int tid, TweetWeight[] pos_term_weights, HashMap<Integer, Double> spatial_hash, double alpha, int k) {

            double total_score = aggregate_score(tid, pos_term_weights, spatial_hash, alpha);
            ResultTweet rt = new ResultTweet(tid, total_score);
            if (local_topk.size() < k) {
                local_topk.add(rt);
            } else {
                double kth = local_topk.peek().get_score();
                if (total_score > kth) {
                    local_topk.remove();
                    local_topk.add(rt);
                }
            }

            return local_topk.peek().get_score();
        }
    }


    //TODO : need to consider the textual and spatial normalizer
    public double aggregate_score(int tid, TweetWeight[] pos_term_weights, HashMap<Integer, Double> spatial_hash, double alpha) {
        double t_score = 0;
        boolean flag = false;
        for (TweetWeight tw : pos_term_weights) {
            if (tw != null) {
                if (tw.get_hash().containsKey(tid)) {
                    flag = true;
                    t_score += tw.get_hash().get(tid);
                }
            }
        }

        if (!flag) {
            return Double.MIN_VALUE;
        }

        double s_score = spatial_hash.get(tid);

        /*if(tid == 731400)
        {
            System.out.println("731400 spatial score is " + (alpha * s_score) + " textial score is " + ((1-alpha) * t_score));
        }*/

        return alpha * s_score + (1 - alpha) * t_score;
    }


    private boolean strings_match(String[] tweet_str, String[] neg_str) {
        if (tweet_str.length == 0) {
            return false;
        }
        int i = 0;
        int j = 0;
        int[] next = getNext(neg_str);
        while (i < tweet_str.length && j < neg_str.length) {
            if (j == -1 || tweet_str[i].equals(neg_str[j])) {
                i++;
                j++;
            } else {
                j = next[j];
            }
        }
        return j == neg_str.length;
    }

    private int[] getNext(String[] str) {
        int[] next = new int[str.length];
        int k = -1;
        int j = 0;
        next[0] = -1;
        while (j < str.length - 1) {
            if (k == -1 || str[j].equals(str[k])) {
                k++;
                j++;
                next[j] = k;
            } else {
                k = next[k];
            }
        }
        return next;
    }
}
         