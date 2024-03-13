package STOPKU.indexing;


import STOPKU.Query.NaiveQueryProcessor;
import STOPKU.Query.QueryInstance;
import STOPKU.Query.QueryInstanceRange;
import STOPKU.Query.OptQueryProcessor;
import STOPKU.Query.ResultTweet;
import STOPKU.disk.ReadFromDisk;
import STOPKU.disk.Tools;
import STOPKU.disk.FlushToDisk;

// for looking at tweet info
import STOPKU.indexing.SpatialTweet;

import java.io.*;
import java.util.*;

import static STOPKU.disk.FlushToDisk.ROOT;


public class BulkLoading {

    public static int default_data_size = 4000000;
    public static int default_depth = 15; //depth of the quadtree
    public static int default_capacity = 30000; //size of a leaf node, i.e., the max number of objects in leaf
    public static double default_alpha = 0.5; //alpha * s_score + (1 - alpha ) * t_score
    public static int default_k = 10; //k is the number of objects that we retrieve
    public static int default_thread_num = 4; //
    public static int default_pos_kws = 3;
    public static int default_neg_num = 1; //the number of negative phrases
    public static int default_neg_len = 2; //the length of the negative phrase

    public static int default_spatial_division = 4; //refers POWER-S
    public static int default_query_num = 1000; //
    public static int default_batch = 100000;
    public static double default_threshold = 1;
    public static long default_spatial_buffer_size = 200000000;

    public static int default_or_kws = 2;

    public static int default_width = 30;
    public static int default_height = 30;


    public static ArrayList<SpatialTweet> test_lists = new ArrayList<>();

    public static void main(String args[]) throws IOException, InterruptedException {

        int[] data_sizes = new int[]{2000000, 4000000};
        int[] depths = new int[]{5 , 10 , 15 ,20 , 25};
        int[] capacities = new int[]{10000, 20000 , 30000, 40000, 50000};
        double[] alphas = new double[]{0 , 0.1 , 0.3 , 0.5 , 0.7 , 0.9 , 1};
        int[] ks = new int[]{5 , 10 , 50 , 100  , 500, 1000, 5000 , 10000};
        int[] thread_nums = new int[]{1 , 2 , 4 , 8};
        int[] spatial_divs = new int[]{5, 10 , 15 , 20 ,25};
        int[] pos_kws = new int[]{1,2,3,4,5};
        int[] neg_seq = new int[]{1,2,3,4,5};
        int[] neg_len = new int[]{1,2,3,4,5};
        int[] or_kws = new int[]{1,2,3,4,5};
        int[] neg_num = new int[]{1,2,3,4 , 5};
        long[] buffer_sizes = new long[]{50000000 , 100000000 , 150000000 ,200000000 , 250000000 , 300000000};
        double[] thresholds = new double[]{0 , 0.1,0.2,0.3,0.4,0.5 , 0.6 ,  0.7,  0.8 ,  0.9 , 1};


        Tools.init();

        // ----- the following performs parameter tuning experiments
        // var_capacity_indexing_time(capacities);
        // var_spatial_division_indexing_time(spatial_divs);
        // var_dsize_indexing_time(data_sizes);
        // var_depth_indexing_time(depths);

        // var_spatial_div_query_time(spatial_divs);
        // var_capacity_query_time(capacities);
        // var_depth_query_time(depths);
        // var_datasize_query_time(data_sizes);
        // var_threshold_query_time(thresholds);
        // var_buffersize_query_time(buffer_sizes);
        // var_threadnum_query_time(thread_nums);
        // default_thread_num = 1;
        // System.out.println("current thread num is " + default_thread_num);
        // var_k_query_time(ks);
        // default_thread_num = 2;
        // System.out.println("current thread num is " + default_thread_num);
        // var_k_query_time(ks);
        // default_thread_num = 4;
        // System.out.println("current thread num is " + default_thread_num);
        // var_k_query_time(ks);
        // default_thread_num = 8;
        // System.out.println("current thread num is " + default_thread_num);
        // var_k_query_time(ks);



       //----------------the following exams the topk-knn query
        // var_pos_query_time(pos_kws );
        // var_neg_query_time(neg_seq);
        // var_negNum_query_time(neg_num);
        // var_k_query_time(ks);
        // var_alpha_query_time(alphas);
        // var_datasize_query_time(data_sizes);

        
        ks = new int[]{10 , 30 , 50 , 70 , 90};
        pos_kws = new int[]{1 ,2,3,4};
        neg_seq = new int[]{1,2,3,4};
        neg_num = new int[]{1,2,3,4};
        or_kws = new int[]{1,2,3,4};
        default_pos_kws = 2;

        int[] width = new int[]{10,20,30,40,50};
        int[] height = new int[]{10,20,30,40,50};
        
        //-----------------the following exams the boolean knn
        // var_andkws_booleanKNN(pos_kws);
        // var_orkws_booleanKNN(or_kws);
        // var_notseq_booleanKNN(neg_seq);
        // var_notNum_booleanKNN(neg_num);
        // var_k_booleanKNN(ks);
        // var_datasize_booleanKNN(data_sizes);

        // range queries
        System.out.println("\t==== var_andkws_boolean_range(pos_kws) ====");
        var_andkws_boolean_range(pos_kws);
        System.out.println("\t==== var_orkws_boolean_range(or_kws) ====");
        var_orkws_boolean_range(or_kws);
        System.out.println("\t==== var_notseq_boolean_range(neg_seq) ====");
        var_notseq_boolean_range(neg_seq);
        System.out.println("\t==== var_notNumeq_boolean_range(neg_num) ====");
        var_notNum_boolean_range(neg_num);
        System.out.println("\t==== var_k_boolean_range(ks) ====");
        var_k_boolean_range(ks);
        System.out.println("\t==== var_datasize_boolean_range(data_sizes) ====");
        var_datasize_boolean_range(data_sizes);

        System.out.println("\t==== var_width_boolean_range(width) ====");
        var_width_boolean_range(width);
        System.out.println("\t==== var_height_boolean_range(height) ====");
        var_height_boolean_range(height);

    }



    public static ArrayList<QueryAttrRange> get_booleanrange_query_inst(int dataset_len , int pos_kw_len , int neg_kw_len , int neg_kew_num ,  int or_kw_len , int num_query, int width, int height) throws IOException {
        ArrayList<QueryAttrRange> qas = new ArrayList<>();

        ArrayList<String[]> and_kws = new ArrayList<>();
        ArrayList<String[]> neg_seq = new ArrayList<>();
        ArrayList<String[]> or_kws = new ArrayList<>();

        ArrayList<Double> lats1 = new ArrayList<>();
        ArrayList<Double> lats2 = new ArrayList<>();
        ArrayList<Double> lons1 = new ArrayList<>();
        ArrayList<Double> lons2 = new ArrayList<>();

        BufferedReader reader = new BufferedReader(new FileReader("pdata/" + "kws"  + dataset_len + "/" + (pos_kw_len + or_kw_len) + ".txt"));
        String line;
        int count = 0;
        while((line = reader.readLine()) != null && !line.equals(""))
        {
            count++;
            if(count == 100)
            {
                break;
            }
            String[] ret = line.split(" ");
            String[] pos_kwd = new String[pos_kw_len];

            for(int i = 0 ; i < pos_kw_len ; i++)
            {
                pos_kwd[i] = ret[i + 2];
            }
            and_kws.add(pos_kwd);

        }
        reader.close();

        reader = new BufferedReader(new FileReader("pdata/" + "kws"  + dataset_len + "/" + (pos_kw_len + or_kw_len) + ".txt"));
        //maybe we need to remove stopwords twice
        count = 0;


        while((line = reader.readLine()) != null && !line.equals(""))
        {
            count++;
            if(count == 100)
            {
                break;
            }
            String[] ret = line.split(" ");
            String[] or_kwd = new String[or_kw_len];

            for(int i = 0 ; i < or_kw_len ; i++)
            {
                or_kwd[i] = ret[i + 2 + pos_kw_len];
            }

            or_kws.add(or_kwd);

        }
        reader.close();


        reader = new BufferedReader(new FileReader("pdata/" + "kws"  + dataset_len + "/" + neg_kw_len + ".txt"));
        //maybe we need to remove stopwords twice
        while((line = reader.readLine()) != null && !line.equals(""))
        {
            String[] ret = line.split(" ");
            String[] neg_kwd = new String[neg_kw_len];
            for(int i = 0 ; i < neg_kw_len ; i++)
            {
                neg_kwd[i] = ret[i + 2];
            }
            neg_seq.add(neg_kwd);
            lats1.add(Double.parseDouble(ret[0]));
            lons1.add(Double.parseDouble(ret[1]));
            //  add max lat and lon, setting 50 for now
            lats2.add((double) Double.parseDouble(ret[0])+height);
            lons2.add((double) Double.parseDouble(ret[1])+width);
        }
        reader.close();


        for(int i = 0 ; i < num_query ; i++)
        {
            int j = i % and_kws.size();
            ArrayList<String[]> neg_phrases = new ArrayList<>();
            for(int p = 0 ; p < neg_kew_num ; p++)
            {
                neg_phrases.add(neg_seq.get(i * neg_kew_num + p));
            }
            // creates new ranged query with a min lat, max lat, min lon, max lon 
            QueryAttrRange qa = new QueryAttrRange(lats1.get(i) , lons1.get(i) , lats2.get(i) , lons2.get(i), and_kws.get(j) , neg_phrases , or_kws.get(j));
            qas.add(qa);
        }


        return qas;

    }


    // ================ Boolean Range Query Evaluation Functions ================
    public static void var_andkws_boolean_range(int[] poskws) throws IOException {
        System.gc();
        System.out.println("-------------------------------------------------");
        System.out.println("exploring how the number of and kws affect query time");
        System.out.println("the following is POWER results");
        String fileName = "andkws";
        for(int pos_len : poskws)
        {
            if(default_k < 100)
            {
                default_thread_num = 1;
            }
            ArrayList<QueryAttrRange> qas = get_booleanrange_query_inst(default_data_size , pos_len , default_neg_len , default_neg_num  , default_or_kws , default_query_num, default_width, default_height);
            OptQueryProcessor qp = new OptQueryProcessor(ReadFromDisk.read_quad_tree(default_data_size , default_capacity , default_depth , default_spatial_division) , default_thread_num, default_spatial_buffer_size);
            long start_time = System.currentTimeMillis();
            for (QueryAttrRange qa : qas)
            {
                QueryInstanceRange qi = new QueryInstanceRange(qa.lat1, qa.lat2, qa.lon1, qa.lon2, qa.pos_keywords, qa.neg_phrases, default_alpha, default_k , qa.or_keywords,  default_threshold);
                qp.process_query_range(qi);
            }
            long end_time = System.currentTimeMillis();
            long avg_time = (end_time - start_time) / qas.size();
            System.out.println("num of and words is " + pos_len + " the average runtime is " + avg_time);
            saveToFile("andkws", pos_len, qas);
            System.out.println("Saved");
        }
        System.gc();
    }

    public static void var_orkws_boolean_range(int[] orkws) throws IOException {
        System.gc();
        System.out.println("-------------------------------------------------");
        System.out.println("exploring how the number of or kws affect query time");
        System.out.println("the following is POWER results");
        for(int or_len : orkws)
        {
            if(default_k < 100)
            {
                default_thread_num = 1;
            }
            ArrayList<QueryAttrRange> qas = get_booleanrange_query_inst(default_data_size , default_pos_kws , default_neg_len, default_neg_num,  or_len , default_query_num, default_width, default_height);
            OptQueryProcessor qp = new OptQueryProcessor(ReadFromDisk.read_quad_tree(default_data_size , default_capacity , default_depth , default_spatial_division) , default_thread_num, default_spatial_buffer_size);
            long start_time = System.currentTimeMillis();
            for (QueryAttrRange qa : qas)
            {
                QueryInstanceRange qi = new QueryInstanceRange(qa.lat1, qa.lat2, qa.lon1, qa.lon2, qa.pos_keywords, qa.neg_phrases, default_alpha, default_k , qa.or_keywords,  default_threshold);
                qp.process_query_range(qi);

            }
            long end_time = System.currentTimeMillis();
            long avg_time = (end_time - start_time) / qas.size();
            System.out.println("num of or words is " + or_len + " the average runtime is " + avg_time);
            saveToFile("orkws", or_len, qas);
            System.out.println("Saved");
        }
        System.gc();
    }

    public static void var_notseq_boolean_range(int[] notseq) throws IOException {
        System.gc();
        System.out.println("-------------------------------------------------");
        System.out.println("exploring how the number of neg kws affect query time");
        System.out.println("the following is POWER results");
        for(int not_len : notseq)
        {
            if(default_k < 100)
            {
                default_thread_num = 1;
            }
            ArrayList<QueryAttrRange> qas = get_booleanrange_query_inst(default_data_size , default_pos_kws , not_len , default_neg_num , default_or_kws , default_query_num, default_width, default_height);
            OptQueryProcessor qp = new OptQueryProcessor(ReadFromDisk.read_quad_tree(default_data_size , default_capacity , default_depth , default_spatial_division) , default_thread_num, default_spatial_buffer_size);
            long start_time = System.currentTimeMillis();
            for (QueryAttrRange qa : qas)
            {
                QueryInstanceRange qi = new QueryInstanceRange(qa.lat1, qa.lat2, qa.lon1, qa.lon2, qa.pos_keywords, qa.neg_phrases, default_alpha, default_k , qa.or_keywords,  default_threshold);
                qp.process_query_range(qi);

            }
            long end_time = System.currentTimeMillis();
            long avg_time = (end_time - start_time) / qas.size();
            System.out.println("negative sequence len is  " + not_len + " the average runtime is " + avg_time);
            saveToFile("notseq", not_len, qas);
            System.out.println("Saved");
        }
        System.gc();
    }

    public static void var_notNum_boolean_range(int[] notNum) throws IOException {
        System.gc();
        System.out.println("-------------------------------------------------");
        System.out.println("exploring how the number of neg kws affect query time");
        System.out.println("the following is POWER results");
        for(int not_num : notNum)
        {
            if(default_k < 100)
            {
                default_thread_num = 1;
            }
            ArrayList<QueryAttrRange> qas = get_booleanrange_query_inst(default_data_size , default_pos_kws , default_neg_len , not_num , default_or_kws , default_query_num, default_width, default_height);
            OptQueryProcessor qp = new OptQueryProcessor(ReadFromDisk.read_quad_tree(default_data_size , default_capacity , default_depth , default_spatial_division) , default_thread_num, default_spatial_buffer_size);
            long start_time = System.currentTimeMillis();
            for (QueryAttrRange qa : qas)
            {
                QueryInstanceRange qi = new QueryInstanceRange(qa.lat1, qa.lat2, qa.lon1, qa.lon2, qa.pos_keywords, qa.neg_phrases, default_alpha, default_k , qa.or_keywords,  default_threshold);
                qp.process_query_range(qi);

            }
            long end_time = System.currentTimeMillis();
            long avg_time = (end_time - start_time) / qas.size();
            System.out.println("negative sequence number is  " + not_num + " the average runtime is " + avg_time);
            saveToFile("notNum", not_num, qas);
            System.out.println("Saved");
        }
        System.gc();
    }

    public static void var_k_boolean_range(int[] ks) throws IOException {
        System.gc();
        System.out.println("-------------------------------------------------");
        System.out.println("exploring how the k affect query time");
        System.out.println("the following is POWER results");
        for (int k : ks) {
            if(k < 100)
            {
                default_thread_num = 1;
            }
            ArrayList<QueryAttrRange> qas = get_booleanrange_query_inst(default_data_size, default_pos_kws, default_neg_len , default_neg_num, default_or_kws , default_query_num, default_width, default_height);
            OptQueryProcessor qp = new OptQueryProcessor(ReadFromDisk.read_quad_tree(default_data_size, default_capacity, default_depth, default_spatial_division), default_thread_num, default_spatial_buffer_size);
            long start_time = System.currentTimeMillis();
            for (QueryAttrRange qa : qas) {
                QueryInstanceRange qi = new QueryInstanceRange(qa.lat1, qa.lat2, qa.lon1, qa.lon2, qa.pos_keywords, qa.neg_phrases, default_alpha, k, qa.or_keywords, default_threshold);
                qp.process_query_range(qi);
            }
            long end_time = System.currentTimeMillis();
            long avg_time = (end_time - start_time) / qas.size();
            System.out.println("current k is " + k + " the average runtime is " + avg_time);
            saveToFile("k", k, qas);
            System.out.println("Saved");
        }
        System.gc();
    }

    public static void var_datasize_boolean_range(int[] datasets) throws IOException {
        System.gc();
        System.out.println("-------------------------------------------------");
        System.out.println("exploring how the size of dataset affect query time");
        System.out.println("the following is POWER");
        for(int dataset : datasets)
        {
            if(default_k < 100)
            {
                default_thread_num = 1;
            }
            ArrayList<QueryAttrRange> qas = get_booleanrange_query_inst(dataset , default_pos_kws , default_neg_len ,  default_neg_num, default_or_kws , default_query_num, default_width, default_height);
            OptQueryProcessor qp = new OptQueryProcessor(ReadFromDisk.read_quad_tree(dataset , default_capacity , default_depth , default_spatial_division) , default_thread_num , default_spatial_buffer_size);
            long start_time = System.currentTimeMillis();
            for (QueryAttrRange qa : qas)
            {
                QueryInstanceRange qi = new QueryInstanceRange(qa.lat1, qa.lat2, qa.lon1, qa.lon2, qa.pos_keywords, qa.neg_phrases, default_alpha , default_k , qa.or_keywords , default_threshold);
                qp.process_query_range(qi);
            }
            long end_time = System.currentTimeMillis();
            long avg_time = (end_time - start_time) / qas.size();
            System.out.println("current dataset is " + dataset + " the average runtime is " + avg_time);
            saveToFile("datasize", dataset, qas);
            System.out.println("Saved");
        }
        System.gc();
    }

    public static void var_width_boolean_range(int[] widths) throws IOException {
        System.gc();
        System.out.println("-------------------------------------------------");
        System.out.println("exploring how the size of the width of the range affect query time");
        System.out.println("the following is POWER results");
        for(int width : widths)
        {
            if(default_k < 100)
            {
                default_thread_num = 1;
            }
            ArrayList<QueryAttrRange> qas = get_booleanrange_query_inst(default_data_size , default_pos_kws , default_neg_len , default_neg_num  , default_or_kws , default_query_num, width, default_height);
            OptQueryProcessor qp = new OptQueryProcessor(ReadFromDisk.read_quad_tree(default_data_size , default_capacity , default_depth , default_spatial_division) , default_thread_num, default_spatial_buffer_size);
            long start_time = System.currentTimeMillis();
            for (QueryAttrRange qa : qas)
            {
                QueryInstanceRange qi = new QueryInstanceRange(qa.lat1, qa.lat2, qa.lon1, qa.lon2, qa.pos_keywords, qa.neg_phrases, default_alpha, default_k , qa.or_keywords,  default_threshold);
                qp.process_query_range(qi);
            }
            long end_time = System.currentTimeMillis();
            long avg_time = (end_time - start_time) / qas.size();
            System.out.println("current width is " + width + " the average runtime is " + avg_time);
            saveToFile("width", width, qas);
            System.out.println("Saved");
        }
        System.gc();
    }

    public static void var_height_boolean_range(int[] heights) throws IOException {
        System.gc();
        System.out.println("-------------------------------------------------");
        System.out.println("exploring how the size of the height of the range affect query time");
        System.out.println("the following is POWER results");
        for(int height : heights)
        {
            if(default_k < 100)
            {
                default_thread_num = 1;
            }
            ArrayList<QueryAttrRange> qas = get_booleanrange_query_inst(default_data_size , default_pos_kws , default_neg_len , default_neg_num  , default_or_kws , default_query_num, default_width, height);
            OptQueryProcessor qp = new OptQueryProcessor(ReadFromDisk.read_quad_tree(default_data_size , default_capacity , default_depth , default_spatial_division) , default_thread_num, default_spatial_buffer_size);
            long start_time = System.currentTimeMillis();
            for (QueryAttrRange qa : qas)
            {
                QueryInstanceRange qi = new QueryInstanceRange(qa.lat1, qa.lat2, qa.lon1, qa.lon2, qa.pos_keywords, qa.neg_phrases, default_alpha, default_k , qa.or_keywords,  default_threshold);
                qp.process_query_range(qi);
            }
            long end_time = System.currentTimeMillis();
            long avg_time = (end_time - start_time) / qas.size();
            System.out.println("current height is " + height + " the average runtime is " + avg_time);
            saveToFile("height", height, qas);
            System.out.println("Saved");
        }
        System.gc();
    } 


    // lat1,lat2,long1,long2|positive,keyrwords|negative,phrase1=negative,phrase2|or,keywords
    public static void saveToFile(String name, int num, ArrayList<QueryAttrRange> qas) {
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter("queries/" + name + num + ".txt"));

            String line = "";
            for(QueryAttrRange qa : qas) {
                // lat1, lat2, lon1, lon2
                line = "" + qa.lat1 + "," + qa.lat2 + "," + qa.lon1 + "," + qa.lon2 + "|";

                // positive keywords
                for(String kwd : qa.pos_keywords) {
                    line = line + kwd + ",";
                }
                line = line.substring(0, line.length()-1);
                line = line + "|";

                // negative phrases
                for(String[] keywords : qa.neg_phrases) {
                    for(String kwd : keywords) {
                        line = line + kwd + ",";
                    }
                    line = line.substring(0, line.length()-1);
                    line = line + "=";
                }
                line = line.substring(0, line.length()-1);
                line = line + "|";

                // or keywords
                for(String kwd : qa.or_keywords) {
                    line = line + kwd + ",";
                }
                line = line.substring(0, line.length()-1);
                
                line = line + "\n";

                writer.write(line);
            }


            writer.close();
        } catch (IOException e) {
            System.err.println("[IOException error] " + name + " " + num);
            e.printStackTrace();
        }
        
    }





    public static void var_pos_query_time(int[] poskws) throws IOException {
        System.gc();
        System.out.println("-------------------------------------------------");
        System.out.println("exploring how the number of positive kws affect query time");
        System.out.println("the following is POWER results");
        for(int pos_len : poskws)
        {
            if(default_k < 100)
            {
                default_thread_num = 1;
            }
            ArrayList<QueryAttr> qas = get_topknn_query_inst(default_data_size , pos_len , default_neg_num, default_neg_len , default_query_num);
            OptQueryProcessor qp = new OptQueryProcessor(ReadFromDisk.read_quad_tree(default_data_size , default_capacity , default_depth , default_spatial_division) , default_thread_num, default_spatial_buffer_size);
            long start_time = System.currentTimeMillis();
            for (QueryAttr qa : qas)
            {
                QueryInstance qi = new QueryInstance(qa.lat, qa.lon, qa.pos_keywords, qa.neg_phrases, default_alpha, default_k , null ,  default_threshold);
                qp.process_query(qi);

            }
            long end_time = System.currentTimeMillis();
            long avg_time = (end_time - start_time) / qas.size();
            System.out.println("num of poskws is " + pos_len + " the average runtime is " + avg_time);
        }
        System.gc();


        System.out.println("the following is POWER-naive results");
        for(int pos_len : poskws)
        {
            ArrayList<QueryAttr> qas = get_topknn_query_inst(default_data_size , pos_len , default_neg_num , default_neg_len , default_query_num);
            NaiveQueryProcessor qp = new NaiveQueryProcessor(ReadFromDisk.read_quad_tree(default_data_size , default_capacity , default_depth , default_spatial_division) , default_thread_num, default_spatial_buffer_size);
            long start_time = System.currentTimeMillis();
            for (QueryAttr qa : qas)
            {
                QueryInstance qi = new QueryInstance(qa.lat, qa.lon, qa.pos_keywords, qa.neg_phrases, default_alpha, default_k , null, default_threshold);
                qp.process_query(qi);

            }
            long end_time = System.currentTimeMillis();
            long avg_time = (end_time - start_time) / qas.size();
            System.out.println("num of poskws is " + pos_len + " the average runtime is " + avg_time);
        }
        System.gc();


        System.out.println("the following is POWER-spatial results");
        for(int pos_len : poskws)
        {
            ArrayList<QueryAttr> qas = get_topknn_query_inst(default_data_size , pos_len , default_neg_num , default_neg_len , default_query_num);
            OptQueryProcessor qp = new OptQueryProcessor(ReadFromDisk.read_quad_tree(default_data_size , default_capacity , default_depth , default_spatial_division) , default_thread_num, default_spatial_buffer_size);
            long start_time = System.currentTimeMillis();
            for (QueryAttr qa : qas)
            {
                QueryInstance qi = new QueryInstance(qa.lat, qa.lon, qa.pos_keywords, qa.neg_phrases, default_alpha, default_k , null, Double.MIN_VALUE);
                qp.process_query(qi);

            }
            long end_time = System.currentTimeMillis();
            long avg_time = (end_time - start_time) / qas.size();
            System.out.println("num of poskws is " + pos_len + " the average runtime is " + avg_time);
        }
        System.out.println("=================================================");
    }

    public static void var_neg_query_time(int[] negkws) throws IOException {
        System.gc();
        System.out.println("-------------------------------------------------");
        System.out.println("exploring how the number of negative kws affect query time");
        System.out.println("the following is POWER results");
        for(int neg_lenth : negkws)
        {
            if(default_k < 100)
            {
                default_thread_num = 1;
            }
            ArrayList<QueryAttr> qas = get_topknn_query_inst(default_data_size , default_pos_kws , default_neg_num,  neg_lenth , default_query_num);
            OptQueryProcessor qp = new OptQueryProcessor(ReadFromDisk.read_quad_tree(default_data_size , default_capacity , default_depth , default_spatial_division) , default_thread_num , default_spatial_buffer_size);
            long start_time = System.currentTimeMillis();
            for (QueryAttr qa : qas)
            {
                QueryInstance qi = new QueryInstance(qa.lat, qa.lon, qa.pos_keywords, qa.neg_phrases, default_alpha, default_k , null , default_threshold);
                qp.process_query(qi);
            }
            long end_time = System.currentTimeMillis();
            long avg_time = (end_time - start_time) / qas.size();
            System.out.println("length of negative sequence is " + neg_lenth + " the average runtime is " + avg_time);
        }
        System.gc();

        System.out.println("the following is POWER-naive results");
        for(int neg_len : negkws)
        {
            ArrayList<QueryAttr> qas = get_topknn_query_inst(default_data_size , default_pos_kws , default_neg_num , neg_len, default_query_num);
            NaiveQueryProcessor qp = new NaiveQueryProcessor(ReadFromDisk.read_quad_tree(default_data_size , default_capacity , default_depth , default_spatial_division) , default_thread_num, default_spatial_buffer_size);
            long start_time = System.currentTimeMillis();
            for (QueryAttr qa : qas)
            {
                QueryInstance qi = new QueryInstance(qa.lat, qa.lon, qa.pos_keywords, qa.neg_phrases, default_alpha, default_k , null, default_threshold);
                qp.process_query(qi);

            }
            long end_time = System.currentTimeMillis();
            long avg_time = (end_time - start_time) / qas.size();
            System.out.println("num of negative kwd is " + neg_len + " the average runtime is " + avg_time);
        }
        System.gc();


        System.out.println("the following is POWER-spatial results");
        for(int neg_len : negkws)
        {
            ArrayList<QueryAttr> qas = get_topknn_query_inst(default_data_size , default_pos_kws , default_neg_num, neg_len , default_query_num);
            OptQueryProcessor qp = new OptQueryProcessor(ReadFromDisk.read_quad_tree(default_data_size , default_capacity , default_depth , default_spatial_division) , default_thread_num, default_spatial_buffer_size);
            long start_time = System.currentTimeMillis();
            for (QueryAttr qa : qas)
            {
                QueryInstance qi = new QueryInstance(qa.lat, qa.lon, qa.pos_keywords, qa.neg_phrases, default_alpha, default_k , null , Double.MIN_VALUE);
                qp.process_query(qi);

            }
            long end_time = System.currentTimeMillis();
            long avg_time = (end_time - start_time) / qas.size();
            System.out.println("num of negative kwd is " + neg_len + " the average runtime is " + avg_time);
        }
        System.gc();


    }

    public static void var_negNum_query_time(int[] negkwsNum) throws IOException {
        System.gc();
        System.out.println("-------------------------------------------------");
        System.out.println("exploring how the number of negative kws affect query time");
        System.out.println("the following is POWER results");
        for(int negNum : negkwsNum)
        {
            if(default_k < 100)
            {
                default_thread_num = 1;
            }
            ArrayList<QueryAttr> qas = get_topknn_query_inst(default_data_size , default_pos_kws , negNum, default_neg_len  , default_query_num);
            OptQueryProcessor qp = new OptQueryProcessor(ReadFromDisk.read_quad_tree(default_data_size , default_capacity , default_depth , default_spatial_division) , default_thread_num , default_spatial_buffer_size);
            long start_time = System.currentTimeMillis();
            for (QueryAttr qa : qas)
            {
                QueryInstance qi = new QueryInstance(qa.lat, qa.lon, qa.pos_keywords, qa.neg_phrases, default_alpha, default_k , null , default_threshold);
                qp.process_query(qi);
            }
            long end_time = System.currentTimeMillis();
            long avg_time = (end_time - start_time) / qas.size();
            System.out.println("length of negative sequence is " + negNum + " the average runtime is " + avg_time);
        }
        System.gc();

        System.out.println("the following is POWER-naive results");
        for(int negNum : negkwsNum)
        {
            ArrayList<QueryAttr> qas = get_topknn_query_inst(default_data_size , default_pos_kws , negNum, default_neg_len, default_query_num);
            NaiveQueryProcessor qp = new NaiveQueryProcessor(ReadFromDisk.read_quad_tree(default_data_size , default_capacity , default_depth , default_spatial_division) , default_thread_num, default_spatial_buffer_size);
            long start_time = System.currentTimeMillis();
            for (QueryAttr qa : qas)
            {
                QueryInstance qi = new QueryInstance(qa.lat, qa.lon, qa.pos_keywords, qa.neg_phrases, default_alpha, default_k , null, default_threshold);
                qp.process_query(qi);

            }
            long end_time = System.currentTimeMillis();
            long avg_time = (end_time - start_time) / qas.size();
            System.out.println("num of negative kwd is " +  negNum + " the average runtime is " + avg_time);
        }
        System.gc();


        System.out.println("the following is POWER-spatial results");
        for(int  negNum  : negkwsNum)
        {
            ArrayList<QueryAttr> qas = get_topknn_query_inst(default_data_size , default_pos_kws ,  negNum  , default_neg_len, default_query_num);
            OptQueryProcessor qp = new OptQueryProcessor(ReadFromDisk.read_quad_tree(default_data_size , default_capacity , default_depth , default_spatial_division) , default_thread_num, default_spatial_buffer_size);
            long start_time = System.currentTimeMillis();
            for (QueryAttr qa : qas)
            {
                QueryInstance qi = new QueryInstance(qa.lat, qa.lon, qa.pos_keywords, qa.neg_phrases, default_alpha, default_k , null , Double.MIN_VALUE);
                qp.process_query(qi);

            }
            long end_time = System.currentTimeMillis();
            long avg_time = (end_time - start_time) / qas.size();
            System.out.println("num of negative kwd is " + negNum + " the average runtime is " + avg_time);
        }
        System.gc();


    }

    public static void var_k_query_time(int[] ks) throws IOException {
        System.gc();
        System.out.println("-------------------------------------------------");
        System.out.println("exploring how the k affect query time");
        System.out.println("the following is POWER results");
        for(int k : ks)
        {
            ArrayList<QueryAttr> qas = get_topknn_query_inst(default_data_size , default_pos_kws , default_neg_num,  default_neg_len , default_query_num);
            OptQueryProcessor qp = new OptQueryProcessor(ReadFromDisk.read_quad_tree(default_data_size , default_capacity , default_depth , default_spatial_division) , default_thread_num , default_spatial_buffer_size);
            long start_time = System.currentTimeMillis();
            for (QueryAttr qa : qas)
            {
                QueryInstance qi = new QueryInstance(qa.lat, qa.lon, qa.pos_keywords, qa.neg_phrases, default_alpha, k , null ,  default_threshold);
                qp.process_query(qi);
            }
            long end_time = System.currentTimeMillis();
            long avg_time = (end_time - start_time) / qas.size();
            System.out.println("current k is " + k + " the average runtime is " + avg_time);
        }
        System.gc();

        System.out.println("the following is POWER-naive results");
        for(int k : ks)
        {
            ArrayList<QueryAttr> qas = get_topknn_query_inst(default_data_size , default_pos_kws , default_neg_num , default_neg_len, default_query_num);
            NaiveQueryProcessor qp = new NaiveQueryProcessor(ReadFromDisk.read_quad_tree(default_data_size , default_capacity , default_depth , default_spatial_division) , default_thread_num, default_spatial_buffer_size);
            long start_time = System.currentTimeMillis();
            for (QueryAttr qa : qas)
            {
                QueryInstance qi = new QueryInstance(qa.lat, qa.lon, qa.pos_keywords, qa.neg_phrases, default_alpha, k , null, default_threshold);
                qp.process_query(qi);

            }
            long end_time = System.currentTimeMillis();
            long avg_time = (end_time - start_time) / qas.size();
            System.out.println("k " + k + " the average runtime is " + avg_time);
        }
        System.gc();


        System.out.println("the following is POWER-spatial results");
        for(int k : ks)
        {
            ArrayList<QueryAttr> qas = get_topknn_query_inst(default_data_size , default_pos_kws , default_neg_num, default_neg_len, default_query_num);
            OptQueryProcessor qp = new OptQueryProcessor(ReadFromDisk.read_quad_tree(default_data_size , default_capacity , default_depth , default_spatial_division) , default_thread_num, default_spatial_buffer_size);
            long start_time = System.currentTimeMillis();
            for (QueryAttr qa : qas)
            {
                QueryInstance qi = new QueryInstance(qa.lat, qa.lon, qa.pos_keywords, qa.neg_phrases, default_alpha, k , null, Double.MIN_VALUE);
                qp.process_query(qi);

            }
            long end_time = System.currentTimeMillis();
            long avg_time = (end_time - start_time) / qas.size();
            System.out.println("k " + k + " the average runtime is " + avg_time);
        }
        System.gc();
    }

    public static void var_alpha_query_time(double[] alphas) throws IOException {
        System.gc();
        System.out.println("-------------------------------------------------");
        System.out.println("exploring how the alpha affect query time");
        System.out.println("the following is POWER results");
        for(double alpha : alphas)
        {
            if(default_k < 100)
            {
                default_thread_num = 1;
            }
            ArrayList<QueryAttr> qas = get_topknn_query_inst(default_data_size , default_pos_kws , default_neg_num, default_neg_len , default_query_num);
            OptQueryProcessor qp = new OptQueryProcessor(ReadFromDisk.read_quad_tree(default_data_size , default_capacity , default_depth , default_spatial_division) , default_thread_num , default_spatial_buffer_size);
            long start_time = System.currentTimeMillis();
            for (QueryAttr qa : qas)
            {
                QueryInstance qi = new QueryInstance(qa.lat, qa.lon, qa.pos_keywords, qa.neg_phrases, alpha, default_k , null , default_threshold);
                qp.process_query(qi);
            }
            long end_time = System.currentTimeMillis();
            long avg_time = (end_time - start_time) / qas.size();
            System.out.println("\n current alpha is " + alpha + " the average runtime is " + avg_time);
        }
        System.gc();

        System.out.println("the following is POWER-naive results");
        for(double alpha : alphas)
        {
            ArrayList<QueryAttr> qas = get_topknn_query_inst(default_data_size , default_pos_kws , default_neg_num , default_neg_len, default_query_num);
            NaiveQueryProcessor qp = new NaiveQueryProcessor(ReadFromDisk.read_quad_tree(default_data_size , default_capacity , default_depth , default_spatial_division) , default_thread_num, default_spatial_buffer_size);
            long start_time = System.currentTimeMillis();
            for (QueryAttr qa : qas)
            {
                QueryInstance qi = new QueryInstance(qa.lat, qa.lon, qa.pos_keywords, qa.neg_phrases, alpha, default_k ,null ,  default_threshold);
                qp.process_query(qi);

            }
            long end_time = System.currentTimeMillis();
            long avg_time = (end_time - start_time) / qas.size();
            System.out.println("current alpha " + alpha + " the average runtime is " + avg_time);
        }
        System.gc();


        System.out.println("the following is POWER-spatial results");
        for(double alpha : alphas)
        {
            ArrayList<QueryAttr> qas = get_topknn_query_inst(default_data_size , default_pos_kws , default_neg_num, default_neg_len, default_query_num);
            OptQueryProcessor qp = new OptQueryProcessor(ReadFromDisk.read_quad_tree(default_data_size , default_capacity , default_depth , default_spatial_division) , default_thread_num, default_spatial_buffer_size);
            long start_time = System.currentTimeMillis();
            for (QueryAttr qa : qas)
            {
                QueryInstance qi = new QueryInstance(qa.lat, qa.lon, qa.pos_keywords, qa.neg_phrases, alpha, default_k , null , Double.MIN_VALUE);
                qp.process_query(qi);

            }
            long end_time = System.currentTimeMillis();
            long avg_time = (end_time - start_time) / qas.size();
            System.out.println("current alpha " + alpha + " the average runtime is " + avg_time);
        }
        System.gc();
    }

    public static void var_threadnum_query_time(int[] threadnums) throws IOException {
        System.gc();
        System.out.println("-------------------------------------------------");
        System.out.println("exploring how the number of thread affect query time" + "thread num is " + default_thread_num);
        System.out.println("the following is POWER results");
        for(int threadnum : threadnums)
        {
            ArrayList<QueryAttr> qas = get_topknn_query_inst(default_data_size , default_pos_kws , default_neg_num,  default_neg_len , default_query_num);
            OptQueryProcessor qp = new OptQueryProcessor(ReadFromDisk.read_quad_tree(default_data_size , default_capacity , default_depth , default_spatial_division) , threadnum , default_spatial_buffer_size);
            long start_time = System.currentTimeMillis();
            for (QueryAttr qa : qas)
            {
                QueryInstance qi = new QueryInstance(qa.lat, qa.lon, qa.pos_keywords, qa.neg_phrases, default_alpha, default_k , null   ,default_threshold);
                qp.process_query(qi);
            }
            long end_time = System.currentTimeMillis();
            long avg_time = (end_time - start_time) / qas.size();
            System.out.println("current thread number is " + threadnum + " the average runtime is " + avg_time);
        }
        System.gc();

        System.out.println("the following is POWER-naive results");
        for(int threadnum : threadnums)
        {
            ArrayList<QueryAttr> qas = get_topknn_query_inst(default_data_size , default_pos_kws , default_neg_num, default_neg_len , default_query_num);
            NaiveQueryProcessor qp = new NaiveQueryProcessor(ReadFromDisk.read_quad_tree(default_data_size , default_capacity , default_depth , default_spatial_division) , threadnum, default_spatial_buffer_size);
            long start_time = System.currentTimeMillis();
            for (QueryAttr qa : qas)
            {
                QueryInstance qi = new QueryInstance(qa.lat, qa.lon, qa.pos_keywords, qa.neg_phrases, default_alpha, default_k , null, default_threshold);
                qp.process_query(qi);

            }
            long end_time = System.currentTimeMillis();
            long avg_time = (end_time - start_time) / qas.size();
            System.out.println("thread is " + threadnum + " the average runtime is " + avg_time);
        }
        System.gc();


        System.out.println("the following is POWER-spatial results");
        for(int threadnum : threadnums)
        {
            ArrayList<QueryAttr> qas = get_topknn_query_inst(default_data_size , default_pos_kws , default_neg_num,  default_neg_len , default_query_num);
            OptQueryProcessor qp = new OptQueryProcessor(ReadFromDisk.read_quad_tree(default_data_size , default_capacity , default_depth , default_spatial_division) , threadnum, default_spatial_buffer_size);
            long start_time = System.currentTimeMillis();
            for (QueryAttr qa : qas)
            {
                QueryInstance qi = new QueryInstance(qa.lat, qa.lon, qa.pos_keywords, qa.neg_phrases, default_alpha, default_k , null , Double.MIN_VALUE);
                qp.process_query(qi);

            }
            long end_time = System.currentTimeMillis();
            long avg_time = (end_time - start_time) / qas.size();
            System.out.println("thread is " + threadnum + " the average runtime is " + avg_time);
        }
        System.gc();
    }

    public static void var_buffersize_query_time(long[] buffer_sizes) throws IOException {
        System.gc();
        System.out.println("-------------------------------------------------");
        System.out.println("exploring how the size of buffer affects the query time");
        System.out.println("the following is POWER");
        for(long buffer_size : buffer_sizes)
        {
            if(default_k < 100)
            {
                default_thread_num = 1;
            }
            ArrayList<QueryAttr> qas = get_topknn_query_inst(default_data_size , default_pos_kws , default_neg_num, default_neg_len ,default_query_num);
            OptQueryProcessor qp = new OptQueryProcessor(ReadFromDisk.read_quad_tree(default_data_size , default_capacity , default_depth , default_spatial_division) , default_thread_num , buffer_size);
            long start_time = System.currentTimeMillis();
            for (QueryAttr qa : qas)
            {
                QueryInstance qi = new QueryInstance(qa.lat, qa.lon, qa.pos_keywords, qa.neg_phrases, default_alpha, default_k , null  ,default_threshold);
                qp.process_query(qi);
            }
            long end_time = System.currentTimeMillis();
            long avg_time = (end_time - start_time) / qas.size();
            System.out.println("current buffer size is " + buffer_size + " the average runtime is " + avg_time);
        }
        System.gc();

        System.out.println("the following is POWER-naive results");
        for(long buffer_size : buffer_sizes)
        {
            if(default_k < 100)
            {
                default_thread_num = 1;
            }
            ArrayList<QueryAttr> qas = get_topknn_query_inst(default_data_size , default_pos_kws , default_neg_num, default_neg_len ,default_query_num);
            NaiveQueryProcessor qp = new NaiveQueryProcessor(ReadFromDisk.read_quad_tree(default_data_size , default_capacity , default_depth , default_spatial_division) , default_thread_num, buffer_size);
            long start_time = System.currentTimeMillis();
            for (QueryAttr qa : qas)
            {
                QueryInstance qi = new QueryInstance(qa.lat, qa.lon, qa.pos_keywords, qa.neg_phrases, default_alpha, default_k , null, default_threshold);
                qp.process_query(qi);

            }
            long end_time = System.currentTimeMillis();
            long avg_time = (end_time - start_time) / qas.size();
            System.out.println("current buffer size is " + buffer_size + " the average runtime is " + avg_time);
        }
        System.gc();


        System.out.println("the following is POWER-spatial results");
        for(long buffer_size : buffer_sizes)
        {
            if(default_k < 100)
            {
                default_thread_num = 1;
            }
            ArrayList<QueryAttr> qas = get_topknn_query_inst(default_data_size , default_pos_kws , default_neg_num, default_neg_len , default_query_num);
            OptQueryProcessor qp = new OptQueryProcessor(ReadFromDisk.read_quad_tree(default_data_size , default_capacity , default_depth , default_spatial_division) , default_thread_num , buffer_size);
            long start_time = System.currentTimeMillis();
            for (QueryAttr qa : qas)
            {
                QueryInstance qi = new QueryInstance(qa.lat, qa.lon, qa.pos_keywords, qa.neg_phrases, default_alpha, default_k , null, Double.MIN_VALUE);
                qp.process_query(qi);

            }
            long end_time = System.currentTimeMillis();
            long avg_time = (end_time - start_time) / qas.size();
            System.out.println("current buffer size is " + buffer_size + " the average runtime is " + avg_time);
        }
        System.gc();
    }

    public static void var_datasize_query_time(int[] datasets) throws IOException {
        System.gc();
        System.out.println("-------------------------------------------------");
        System.out.println("exploring how the size of dataset affect query time");
        System.out.println("the following is POWER");
        for(int dataset : datasets)
        {
            if(default_k < 100)
            {
                default_thread_num = 1;
            }
            ArrayList<QueryAttr> qas = get_topknn_query_inst(dataset , default_pos_kws , default_neg_num, default_neg_len , default_query_num);
            OptQueryProcessor qp = new OptQueryProcessor(ReadFromDisk.read_quad_tree(dataset , default_capacity , default_depth , default_spatial_division) , default_thread_num , default_spatial_buffer_size);
            long start_time = System.currentTimeMillis();
            for (QueryAttr qa : qas)
            {
                QueryInstance qi = new QueryInstance(qa.lat, qa.lon, qa.pos_keywords, qa.neg_phrases, default_alpha , default_k , null , default_threshold);
                qp.process_query(qi);
            }
            long end_time = System.currentTimeMillis();
            long avg_time = (end_time - start_time) / qas.size();
            System.out.println("current dataset is " + dataset + " the average runtime is " + avg_time);
        }
        System.gc();

        System.out.println("the following is POWER-naive results");
        for(int dataset : datasets)
        {
            ArrayList<QueryAttr> qas = get_topknn_query_inst(dataset , default_pos_kws , default_neg_num , default_neg_len, default_query_num);
            NaiveQueryProcessor qp = new NaiveQueryProcessor(ReadFromDisk.read_quad_tree(dataset , default_capacity , default_depth , default_spatial_division) , default_thread_num, default_spatial_buffer_size);
            long start_time = System.currentTimeMillis();
            for (QueryAttr qa : qas)
            {
                QueryInstance qi = new QueryInstance(qa.lat, qa.lon, qa.pos_keywords, qa.neg_phrases, default_alpha, default_k , null, default_threshold);
                qp.process_query(qi);

            }
            long end_time = System.currentTimeMillis();
            long avg_time = (end_time - start_time) / qas.size();
            System.out.println("current dataset is " + dataset + " the average runtime is " + avg_time);
        }
        System.gc();


        System.out.println("the following is POWER-spatial results");
        for(int dataset : datasets)
        {
            ArrayList<QueryAttr> qas = get_topknn_query_inst(dataset , default_pos_kws , default_neg_num , default_neg_len , default_query_num);
            OptQueryProcessor qp = new OptQueryProcessor(ReadFromDisk.read_quad_tree(dataset , default_capacity , default_depth , default_spatial_division) , default_thread_num , default_spatial_buffer_size);
            long start_time = System.currentTimeMillis();
            for (QueryAttr qa : qas)
            {
                QueryInstance qi = new QueryInstance(qa.lat, qa.lon, qa.pos_keywords, qa.neg_phrases, default_alpha, default_k , null, Double.MIN_VALUE);
                qp.process_query(qi);

            }
            long end_time = System.currentTimeMillis();
            long avg_time = (end_time - start_time) / qas.size();
            System.out.println("current dataset is " + dataset + " the average runtime is " + avg_time);
        }
        System.gc();
    }

    public static void var_dsize_indexing_time(int[] data_sizes) throws IOException
    {
        System.gc();
        System.out.println("exploring how dataset size affect the indexing time");
        for(int data_size : data_sizes)
        {

            System.gc();
            String dir = FlushToDisk.BASE + (data_size / 1000000) + "m" + "c" + default_capacity + "d" + default_depth + "d" + default_spatial_division;
            if(new File(dir).isDirectory())
            {
                delete_dir(new File(dir));
                System.out.println("delete completes");
            }
            long start = System.currentTimeMillis();
            indexing("pdata/tweet"+data_size , default_capacity , default_depth , default_batch , data_size , default_spatial_division);
            long end = System.currentTimeMillis();
            System.out.println("the total time indexing is " + (end - start) + " the dataset is " + data_size );
        }
    }

    public static void var_depth_indexing_time(int[] depths) throws IOException
    {
        System.gc();
        System.out.println("exploring how depth of quadtree affects the indexing time");
        for(int depth : depths)
        {
            Node.current_node_id = 0;
            Node.current_depth = 0;
            System.gc();
            String dir = FlushToDisk.BASE + (default_data_size / 1000000) + "m" + "c" + default_capacity + "d" + depth + "d" + default_spatial_division;
            if(new File(dir).isDirectory())
            {
                delete_dir(new File(dir));
                System.out.println("delete completes");
            }

            long start = System.currentTimeMillis();
            indexing("pdata/tweet"+default_data_size , default_capacity , depth , default_batch , default_data_size , default_spatial_division);
            long end = System.currentTimeMillis();
            System.out.println("the total time indexing is " + (end - start) + " the depth " + depth );
        }
    }

    public static void var_depth_query_time(int[] depths) throws IOException {
        System.gc();
        System.out.println("-------------------------------------------------");
        System.out.println("exploring how the depth affects the query time");
        System.out.println("the following is POWER");
        for(int depth : depths)
        {
            if(default_k < 100)
            {
                default_thread_num = 1;
            }
            ArrayList<QueryAttr> qas = get_topknn_query_inst(default_data_size , default_pos_kws , default_neg_num, default_neg_len , default_query_num);
            OptQueryProcessor qp = new OptQueryProcessor(ReadFromDisk.read_quad_tree(default_data_size , default_capacity , depth , default_spatial_division) , default_thread_num , default_spatial_buffer_size);
            long start_time = System.currentTimeMillis();
            for (QueryAttr qa : qas)
            {
                QueryInstance qi = new QueryInstance(qa.lat, qa.lon, qa.pos_keywords, qa.neg_phrases, default_alpha, default_k , null  ,default_threshold);
                qp.process_query(qi);
            }
            long end_time = System.currentTimeMillis();
            long avg_time = (end_time - start_time) / qas.size();
            System.out.println("current depth is " + depth + " the average runtime is " + avg_time);
        }
        System.gc();


        System.out.println("the following is POWER-naive results");
        for(int depth : depths)
        {
            if(default_k < 100)
            {
                default_thread_num = 1;
            }
            ArrayList<QueryAttr> qas = get_topknn_query_inst(default_data_size , default_pos_kws , default_neg_num,default_neg_len, default_query_num);
            NaiveQueryProcessor qp = new NaiveQueryProcessor(ReadFromDisk.read_quad_tree(default_data_size , default_capacity , depth , default_spatial_division) , default_thread_num, default_spatial_buffer_size);
            long start_time = System.currentTimeMillis();
            for (QueryAttr qa : qas)
            {
                QueryInstance qi = new QueryInstance(qa.lat, qa.lon, qa.pos_keywords, qa.neg_phrases, default_alpha, default_k , null, default_threshold);
                qp.process_query(qi);

            }
            long end_time = System.currentTimeMillis();
            long avg_time = (end_time - start_time) / qas.size();
            System.out.println("current depth is " + depth + " the average runtime is " + avg_time);
        }
        System.gc();


        System.out.println("the following is POWER-spatial results");
        for(int depth : depths)
        {
            if(default_k < 100)
            {
                default_thread_num = 1;
            }
            ArrayList<QueryAttr> qas = get_topknn_query_inst(default_data_size , default_pos_kws , default_neg_num, default_neg_len,default_query_num);
            OptQueryProcessor qp = new OptQueryProcessor(ReadFromDisk.read_quad_tree(default_data_size , default_capacity , depth , default_spatial_division) , default_thread_num , default_spatial_buffer_size);
            long start_time = System.currentTimeMillis();
            for (QueryAttr qa : qas)
            {
                QueryInstance qi = new QueryInstance(qa.lat, qa.lon, qa.pos_keywords, qa.neg_phrases, default_alpha, default_k , null, Double.MIN_VALUE);
                qp.process_query(qi);

            }
            long end_time = System.currentTimeMillis();
            long avg_time = (end_time - start_time) / qas.size();
            System.out.println("current depth is " + depth + " the average runtime is " + avg_time);
        }
        System.gc();
    }

    public static void var_capacity_indexing_time(int[] capacities) throws IOException {
        System.gc();
        System.out.println("exploring how capacity of quadtree node affects the indexing time");

        for(int capacity : capacities)
        {
            Node.current_node_id = 0;
            Node.current_depth = 0;
            System.gc();
            String dir = FlushToDisk.BASE + (default_data_size / 1000000) + "m" + "c" + capacity + "d" + default_depth + "d" + default_spatial_division;
            if(new File(dir).isDirectory())
            {
                delete_dir(new File(dir));
                System.out.println("delete completes");
            }
            long start = System.currentTimeMillis();
            indexing("pdata/tweet"+default_data_size , capacity , default_depth , default_batch , default_data_size , default_spatial_division);
            long end = System.currentTimeMillis();
            System.out.println("the total time indexing is " + (end - start) + " the capacity " + capacity );
        }
    }

    public static void var_capacity_query_time(int[] capacities) throws IOException {
        System.gc();
        System.out.println("-------------------------------------------------");
        System.out.println("exploring how capacity of quadtree node affects the query time");
        System.out.println("the following is POWER");
        for(int capacity : capacities)
        {
            System.gc();
            if(default_k < 100)
            {
                default_thread_num = 1;
            }
            ArrayList<QueryAttr> qas = get_topknn_query_inst(default_data_size , default_pos_kws , default_neg_num, default_neg_len , default_query_num);
            OptQueryProcessor qp = new OptQueryProcessor(ReadFromDisk.read_quad_tree(default_data_size , capacity , default_depth , default_spatial_division) , default_thread_num , default_spatial_buffer_size);
            long start_time = System.currentTimeMillis();
            for (QueryAttr qa : qas)
            {
                QueryInstance qi = new QueryInstance(qa.lat, qa.lon, qa.pos_keywords, qa.neg_phrases, default_alpha, default_k , null  ,default_threshold);
                qp.process_query(qi);
            }
            long end_time = System.currentTimeMillis();
            long avg_time = (end_time - start_time) / qas.size();
            System.out.println("current capacity is " + capacity + " the average runtime is " + avg_time);
        }


        System.gc();


        System.out.println("the following is POWER-naive results");
        for(int capacity : capacities)
        {
            if(default_k < 100)
            {
                default_thread_num = 1;
            }
            ArrayList<QueryAttr> qas = get_topknn_query_inst(default_data_size , default_pos_kws , default_neg_num, default_neg_len ,default_query_num);
            NaiveQueryProcessor qp = new NaiveQueryProcessor(ReadFromDisk.read_quad_tree(default_data_size , capacity , default_depth , default_spatial_division) , default_thread_num, default_spatial_buffer_size);
            long start_time = System.currentTimeMillis();
            for (QueryAttr qa : qas)
            {
                QueryInstance qi = new QueryInstance(qa.lat, qa.lon, qa.pos_keywords, qa.neg_phrases, default_alpha, default_k , null , default_threshold);
                qp.process_query(qi);

            }
            long end_time = System.currentTimeMillis();
            long avg_time = (end_time - start_time) / qas.size();
            System.out.println("current capacity is " + capacity + " the average runtime is " + avg_time);
        }
        System.gc();


        System.out.println("the following is POWER-spatial results");
        for(int capacity : capacities)
        {
            if(default_k < 100)
            {
                default_thread_num = 1;
            }
            ArrayList<QueryAttr> qas = get_topknn_query_inst(default_data_size , default_pos_kws , default_neg_num, default_neg_len , default_query_num);
            OptQueryProcessor qp = new OptQueryProcessor(ReadFromDisk.read_quad_tree(default_data_size , capacity , default_depth , default_spatial_division) , default_thread_num , default_spatial_buffer_size);
            long start_time = System.currentTimeMillis();
            for (QueryAttr qa : qas)
            {
                QueryInstance qi = new QueryInstance(qa.lat, qa.lon, qa.pos_keywords, qa.neg_phrases, default_alpha, default_k , null , Double.MIN_VALUE);
                qp.process_query(qi);

            }
            long end_time = System.currentTimeMillis();
            long avg_time = (end_time - start_time) / qas.size();
            System.out.println("current capacity is " + capacity + " the average runtime is " + avg_time);
        }
        System.gc();
    }

    public static void var_spatial_division_indexing_time(int[] spatial_division) throws IOException {
        System.gc();
        System.out.println("exploring how spatial_division  affects the indexing time");

        for(int spatial_div : spatial_division)
        {
            System.gc();
            String dir = FlushToDisk.BASE + (default_data_size / 1000000) + "m" + "c" + default_capacity + "d" + default_depth + "d" + spatial_div;
            if(new File(dir).isDirectory())
            {
                delete_dir(new File(dir));
                System.out.println("delete completes");
            }
            long start = System.currentTimeMillis();
            indexing("pdata/tweet"+default_data_size , default_capacity , default_depth , default_batch , default_data_size , spatial_div);
            long end = System.currentTimeMillis();
            System.out.println("the total time indexing is " + (end - start) + " the division " + spatial_div );
        }
    }

    public static void var_spatial_div_query_time(int[] spatial_division) throws IOException {
        System.gc();
        System.out.println("-------------------------------------------------");
        System.out.println("exploring how spatial division affects the query time");
        System.out.println("the following is POWER");


        System.out.println("the following is POWER-spatial results");
        for(int spatial_div : spatial_division)
        {
            if(default_k < 100)
            {
                default_thread_num = 1;
            }
            ArrayList<QueryAttr> qas = get_topknn_query_inst(default_data_size , default_pos_kws , default_neg_num, default_neg_len , default_query_num);
            OptQueryProcessor qp = new OptQueryProcessor(ReadFromDisk.read_quad_tree(default_data_size , default_capacity , default_depth , spatial_div) , default_thread_num , default_spatial_buffer_size);
            long start_time = System.currentTimeMillis();
            for (QueryAttr qa : qas)
            {
                QueryInstance qi = new QueryInstance(qa.lat, qa.lon, qa.pos_keywords, qa.neg_phrases, default_alpha, default_k , null, Double.MIN_VALUE);
                qp.process_query(qi);

            }
            long end_time = System.currentTimeMillis();
            long avg_time = (end_time - start_time) / qas.size();
            System.out.println("current division is " + spatial_div + " the average runtime is " + avg_time);
        }
        System.gc();
    }

    public static void var_threshold_query_time(double[] thresholds) throws IOException {
        System.gc();
        System.out.println("-------------------------------------------------");
        System.out.println("exploring how threshold affect query time");
        ArrayList<QueryAttr> qas = get_topknn_query_inst(default_data_size , default_pos_kws , default_neg_num, default_neg_len , default_query_num);
        OptQueryProcessor qp = new OptQueryProcessor(ReadFromDisk.read_quad_tree(default_data_size , default_capacity , default_depth , default_spatial_division) , default_thread_num , default_spatial_buffer_size);
        ArrayList<Double> alphas = new ArrayList<>();

        for(int i = 0 ; i < default_query_num ; i++)
        {
            alphas.add(Math.random());
        }

        for(double threshold : thresholds)
        {
            if(default_k < 100)
            {
                default_thread_num = 1;
            }
            long start_time = System.currentTimeMillis();
            for (int i = 0 ; i < qas.size() ; i++)
            {
                QueryAttr qa = qas.get(i);
                QueryInstance qi = new QueryInstance(qa.lat, qa.lon, qa.pos_keywords, qa.neg_phrases, alphas.get(i), default_k , null  ,threshold);
                qp.process_query(qi);
            }
            long end_time = System.currentTimeMillis();
            long avg_time = (end_time - start_time) / qas.size();
            System.out.println("current threshold is " + threshold + " the average runtime is " + avg_time);
        }
        System.out.println("=================================================");
    }






    //-----------------------------------------------------------------
    public static void var_andkws_booleanKNN(int[] poskws) throws IOException {
        System.gc();
        System.out.println("-------------------------------------------------");
        System.out.println("exploring how the number of and kws affect query time");
        System.out.println("the following is POWER results");
        for(int pos_len : poskws)
        {
            if(default_k < 100)
            {
                default_thread_num = 1;
            }
            ArrayList<QueryAttr> qas = get_booleanknn_query_inst(default_data_size , pos_len , default_neg_len , default_neg_num  , default_or_kws , default_query_num);
            OptQueryProcessor qp = new OptQueryProcessor(ReadFromDisk.read_quad_tree(default_data_size , default_capacity , default_depth , default_spatial_division) , default_thread_num, default_spatial_buffer_size);
            long start_time = System.currentTimeMillis();
            for (QueryAttr qa : qas)
            {
                QueryInstance qi = new QueryInstance(qa.lat, qa.lon, qa.pos_keywords, qa.neg_phrases, default_alpha, default_k , qa.or_keywords,  default_threshold);
                qp.process_query(qi);
            }
            long end_time = System.currentTimeMillis();
            long avg_time = (end_time - start_time) / qas.size();
            System.out.println("num of and words is " + pos_len + " the average runtime is " + avg_time);
        }
        System.gc();
    }


    public static void var_orkws_booleanKNN(int[] orkws) throws IOException {
        System.gc();
        System.out.println("-------------------------------------------------");
        System.out.println("exploring how the number of or kws affect query time");
        System.out.println("the following is POWER results");
        for(int or_len : orkws)
        {
            if(default_k < 100)
            {
                default_thread_num = 1;
            }
            ArrayList<QueryAttr> qas = get_booleanknn_query_inst(default_data_size , default_pos_kws , default_neg_len, default_neg_num,  or_len , default_query_num);
            OptQueryProcessor qp = new OptQueryProcessor(ReadFromDisk.read_quad_tree(default_data_size , default_capacity , default_depth , default_spatial_division) , default_thread_num, default_spatial_buffer_size);
            long start_time = System.currentTimeMillis();
            for (QueryAttr qa : qas)
            {
                QueryInstance qi = new QueryInstance(qa.lat, qa.lon, qa.pos_keywords, qa.neg_phrases, default_alpha, default_k , qa.or_keywords,  default_threshold);
                qp.process_query(qi);

            }
            long end_time = System.currentTimeMillis();
            long avg_time = (end_time - start_time) / qas.size();
            System.out.println("num of or words is " + or_len + " the average runtime is " + avg_time);
        }
        System.gc();
    }

    public static void var_notseq_booleanKNN(int[] notseq) throws IOException {
        System.gc();
        System.out.println("-------------------------------------------------");
        System.out.println("exploring how the number of neg kws affect query time");
        System.out.println("the following is POWER results");
        for(int not_len : notseq)
        {
            if(default_k < 100)
            {
                default_thread_num = 1;
            }
            ArrayList<QueryAttr> qas = get_booleanknn_query_inst(default_data_size , default_pos_kws , not_len , default_neg_num , default_or_kws , default_query_num);
            OptQueryProcessor qp = new OptQueryProcessor(ReadFromDisk.read_quad_tree(default_data_size , default_capacity , default_depth , default_spatial_division) , default_thread_num, default_spatial_buffer_size);
            long start_time = System.currentTimeMillis();
            for (QueryAttr qa : qas)
            {
                QueryInstance qi = new QueryInstance(qa.lat, qa.lon, qa.pos_keywords, qa.neg_phrases, default_alpha, default_k , qa.or_keywords,  default_threshold);
                qp.process_query(qi);

            }
            long end_time = System.currentTimeMillis();
            long avg_time = (end_time - start_time) / qas.size();
            System.out.println("negative sequence len is  " + not_len + " the average runtime is " + avg_time);
        }
        System.gc();
    }

    public static void var_notNum_booleanKNN(int[] notNum) throws IOException {
        System.gc();
        System.out.println("-------------------------------------------------");
        System.out.println("exploring how the number of neg kws affect query time");
        System.out.println("the following is POWER results");
        for(int not_num : notNum)
        {
            if(default_k < 100)
            {
                default_thread_num = 1;
            }
            ArrayList<QueryAttr> qas = get_booleanknn_query_inst(default_data_size , default_pos_kws , default_neg_len , not_num , default_or_kws , default_query_num);
            OptQueryProcessor qp = new OptQueryProcessor(ReadFromDisk.read_quad_tree(default_data_size , default_capacity , default_depth , default_spatial_division) , default_thread_num, default_spatial_buffer_size);
            long start_time = System.currentTimeMillis();
            for (QueryAttr qa : qas)
            {
                QueryInstance qi = new QueryInstance(qa.lat, qa.lon, qa.pos_keywords, qa.neg_phrases, default_alpha, default_k , qa.or_keywords,  default_threshold);
                qp.process_query(qi);

            }
            long end_time = System.currentTimeMillis();
            long avg_time = (end_time - start_time) / qas.size();
            System.out.println("negative sequence number is  " + not_num + " the average runtime is " + avg_time);
        }
        System.gc();
    }

    public static void var_k_booleanKNN(int[] ks) throws IOException {
        System.gc();
        System.out.println("-------------------------------------------------");
        System.out.println("exploring how the k affect query time");
        System.out.println("the following is POWER results");
        for (int k : ks) {
            if(k < 100)
            {
                default_thread_num = 1;
            }
            ArrayList<QueryAttr> qas = get_booleanknn_query_inst(default_data_size, default_pos_kws, default_neg_len , default_neg_num, default_or_kws , default_query_num);
            OptQueryProcessor qp = new OptQueryProcessor(ReadFromDisk.read_quad_tree(default_data_size, default_capacity, default_depth, default_spatial_division), default_thread_num, default_spatial_buffer_size);
            long start_time = System.currentTimeMillis();
            for (QueryAttr qa : qas) {
                QueryInstance qi = new QueryInstance(qa.lat, qa.lon, qa.pos_keywords, qa.neg_phrases, default_alpha, k, qa.or_keywords, default_threshold);
                qp.process_query(qi);
            }
            long end_time = System.currentTimeMillis();
            long avg_time = (end_time - start_time) / qas.size();
            System.out.println("current k is " + k + " the average runtime is " + avg_time);
        }
        System.gc();
    }

    public static void var_datasize_booleanKNN(int[] datasets) throws IOException {
        System.gc();
        System.out.println("-------------------------------------------------");
        System.out.println("exploring how the size of dataset affect query time");
        System.out.println("the following is POWER");
        for(int dataset : datasets)
        {
            if(default_k < 100)
            {
                default_thread_num = 1;
            }
            ArrayList<QueryAttr> qas = get_booleanknn_query_inst(dataset , default_pos_kws , default_neg_len ,  default_neg_num, default_or_kws , default_query_num);
            OptQueryProcessor qp = new OptQueryProcessor(ReadFromDisk.read_quad_tree(dataset , default_capacity , default_depth , default_spatial_division) , default_thread_num , default_spatial_buffer_size);
            long start_time = System.currentTimeMillis();
            for (QueryAttr qa : qas)
            {
                QueryInstance qi = new QueryInstance(qa.lat, qa.lon, qa.pos_keywords, qa.neg_phrases, default_alpha , default_k , qa.or_keywords , default_threshold);
                qp.process_query(qi);
            }
            long end_time = System.currentTimeMillis();
            long avg_time = (end_time - start_time) / qas.size();
            System.out.println("current dataset is " + dataset + " the average runtime is " + avg_time);
        }
        System.gc();

        /*System.out.println("the following is POWER-naive results");
        for(int dataset : datasets)
        {
            ArrayList<QueryAttr> qas = get_topknn_query_inst(dataset , default_pos_kws , default_neg_sequence , default_query_num);
            NaiveQueryProcessor qp = new NaiveQueryProcessor(ReadFromDisk.read_quad_tree(dataset , default_capacity , default_depth , default_spatial_division) , default_thread_num, default_spatial_buffer_size);
            long start_time = System.currentTimeMillis();
            for (QueryAttr qa : qas)
            {
                QueryInstance qi = new QueryInstance(qa.lat, qa.lon, qa.pos_keywords, qa.neg_sequence, default_alpha, default_k , null, default_threshold);
                qp.process_query(qi);

            }
            long end_time = System.currentTimeMillis();
            long avg_time = (end_time - start_time) / qas.size();
            System.out.println("current dataset is " + dataset + " the average runtime is " + avg_time);
        }
        System.gc();


        System.out.println("the following is POWER-spatial results");
        for(int dataset : datasets)
        {
            ArrayList<QueryAttr> qas = get_topknn_query_inst(dataset , default_pos_kws , default_neg_sequence , default_query_num);
            OptQueryProcessor qp = new OptQueryProcessor(ReadFromDisk.read_quad_tree(dataset , default_capacity , default_depth , default_spatial_division) , default_thread_num , default_spatial_buffer_size);
            long start_time = System.currentTimeMillis();
            for (QueryAttr qa : qas)
            {
                QueryInstance qi = new QueryInstance(qa.lat, qa.lon, qa.pos_keywords, qa.neg_sequence, default_alpha, default_k , null, Double.MIN_VALUE);
                qp.process_query(qi);

            }
            long end_time = System.currentTimeMillis();
            long avg_time = (end_time - start_time) / qas.size();
            System.out.println("current dataset is " + dataset + " the average runtime is " + avg_time);
        }
        System.gc();*/
    }

    static class QueryAttr
    {
        double lat;
        double lon;
        String[] pos_keywords;
        ArrayList<String[]> neg_phrases;
        String[] or_keywords;
        public QueryAttr(double lat , double lon, String[] pos_keywords , ArrayList<String[]> neg_phrases)
        {
            this.lat = lat;
            this.lon = lon;
            this.pos_keywords = pos_keywords;
            this.neg_phrases = neg_phrases;
        }

        public QueryAttr(double lat , double lon, String[] pos_keywords , ArrayList<String[]> neg_phrases, String[] or_keywords)
        {
            this.lat = lat;
            this.lon = lon;
            this.pos_keywords = pos_keywords;
            this.neg_phrases = neg_phrases;
            this.or_keywords = or_keywords;
        }
    }

    public static ArrayList<QueryAttr> get_topknn_query_inst(int dataset_len , int pos_kw_len , int neg_phrase_num , int neg_phrase_len , int num_query) throws IOException {
        ArrayList<QueryAttr> qas = new ArrayList<>();

        ArrayList<String[]> pos_kws = new ArrayList<>();
        ArrayList<String[]> neg_seq = new ArrayList<>();
        ArrayList<Double> lats = new ArrayList<>();
        ArrayList<Double> lons = new ArrayList<>();
        BufferedReader reader = new BufferedReader(new FileReader("pdata/" + "kws"  + dataset_len + "/" + pos_kw_len + ".txt"));
        String line;
        //maybe we need to remove stopwords twice
        while((line = reader.readLine()) != null && !line.equals(""))
        {
            String[] ret = line.split(" ");
            String[] pos_kwd = new String[pos_kw_len];
            for(int i = 0 ; i < pos_kw_len ; i++)
            {
                pos_kwd[i] = ret[i + 2];
            }
            pos_kws.add(pos_kwd);
            lats.add(Double.parseDouble(ret[0]));
            lons.add(Double.parseDouble(ret[1]));
        }
        reader.close();


        reader = new BufferedReader(new FileReader("pdata/" + "kws"  + dataset_len + "/" + neg_phrase_num + ".txt"));
        //maybe we need to remove stopwords twice
        while((line = reader.readLine()) != null && !line.equals(""))
        {
            String[] ret = line.split(" ");
            String[] neg_kwd = new String[neg_phrase_num];
            for(int i = 0 ; i < neg_phrase_num ; i++)
            {
                neg_kwd[i] = ret[i + 2];
            }
            neg_seq.add(neg_kwd);
        }
        reader.close();

        for(int i = 0 ; i < num_query ; i++)
        {
            ArrayList<String[]> neg_phrases = new ArrayList<>();
            for(int j = 0 ; j < neg_phrase_len ; j++)
            {
                neg_phrases.add(neg_seq.get(i * neg_phrase_len + j));
            }
            QueryAttr qa = new QueryAttr(lats.get(i) , lons.get(i) ,  pos_kws.get(i) , neg_phrases);
            qas.add(qa);
        }


        return qas;

    }

    public static ArrayList<QueryAttr> get_booleanknn_query_inst(int dataset_len , int pos_kw_len , int neg_kw_len , int neg_kew_num ,  int or_kw_len , int num_query) throws IOException {
        ArrayList<QueryAttr> qas = new ArrayList<>();

        ArrayList<String[]> and_kws = new ArrayList<>();
        ArrayList<String[]> neg_seq = new ArrayList<>();
        ArrayList<String[]> or_kws = new ArrayList<>();

        ArrayList<Double> lats = new ArrayList<>();
        ArrayList<Double> lons = new ArrayList<>();
        BufferedReader reader = new BufferedReader(new FileReader("pdata/" + "kws"  + dataset_len + "/" + (pos_kw_len + or_kw_len) + ".txt"));
        String line;
        int count = 0;
        while((line = reader.readLine()) != null && !line.equals(""))
        {
            count++;
            if(count == 100)
            {
                break;
            }
            String[] ret = line.split(" ");
            String[] pos_kwd = new String[pos_kw_len];

            for(int i = 0 ; i < pos_kw_len ; i++)
            {
                pos_kwd[i] = ret[i + 2];
            }
            and_kws.add(pos_kwd);

        }
        reader.close();

        reader = new BufferedReader(new FileReader("pdata/" + "kws"  + dataset_len + "/" + (pos_kw_len + or_kw_len) + ".txt"));
        //maybe we need to remove stopwords twice
        count = 0;


        while((line = reader.readLine()) != null && !line.equals(""))
        {
            count++;
            if(count == 100)
            {
                break;
            }
            String[] ret = line.split(" ");
            String[] or_kwd = new String[or_kw_len];

            for(int i = 0 ; i < or_kw_len ; i++)
            {
                or_kwd[i] = ret[i + 2 + pos_kw_len];
            }

            or_kws.add(or_kwd);

        }
        reader.close();


        reader = new BufferedReader(new FileReader("pdata/" + "kws"  + dataset_len + "/" + neg_kw_len + ".txt"));
        //maybe we need to remove stopwords twice
        while((line = reader.readLine()) != null && !line.equals(""))
        {
            String[] ret = line.split(" ");
            String[] neg_kwd = new String[neg_kw_len];
            for(int i = 0 ; i < neg_kw_len ; i++)
            {
                neg_kwd[i] = ret[i + 2];
            }
            neg_seq.add(neg_kwd);
            lats.add(Double.parseDouble(ret[0]));
            lons.add(Double.parseDouble(ret[1]));
        }
        reader.close();


        for(int i = 0 ; i < num_query ; i++)
        {
            int j = i % and_kws.size();
            ArrayList<String[]> neg_phrases = new ArrayList<>();
            for(int p = 0 ; p < neg_kew_num ; p++)
            {
                neg_phrases.add(neg_seq.get(i * neg_kew_num + p));
            }
            QueryAttr qa = new QueryAttr(lats.get(i) , lons.get(i) ,  and_kws.get(j) , neg_phrases , or_kws.get(j));
            qas.add(qa);
        }


        return qas;

    }

    private static boolean strings_match(String[] tweet_string , String[] neg_str)
    {
        if(tweet_string.length == 0)
        {
            return false;
        }
        int i = 0;
        int j = 0;
        int[] next = getNext(neg_str);
        while (i < tweet_string.length && j < neg_str.length) {
            if (j == -1 || tweet_string[i].equals(neg_str[j]))
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

    private static int[] getNext(String[] str)
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

    public static void naive_test(String dir , boolean AND , double query_lat , double query_lon , String[] pos_kws , String[] neg_kws , double alpha) throws IOException {
        TweetGeneral[] tgs = new TweetGeneral[4000000];
        PriorityQueue<ResultTweet> pq = new PriorityQueue<>((o1, o2) -> Double.compare(o2.get_score() , o1.get_score()));
        int count = 0;
        File f_dir = new File(dir);




            BufferedReader reader = null;
            for(String subfile : Objects.requireNonNull(f_dir.list()))
            {
                reader = new BufferedReader(new FileReader(f_dir + "/" + subfile));
                String line;
                System.out.println("the current file is " + (f_dir + "/" + subfile) + " the current count is " + count);
                //maybe we need to remove stopwords twice
                while((line = reader.readLine()) != null && !line.equals(""))
                {
                    String[] ret = line.split(" ");
                    if(ret != null)
                    {
                        count++;
                        int oid = Integer.parseInt(ret[0]);
                        double lat = Double.parseDouble(ret[1]);
                        double lon = Double.parseDouble(ret[2]);
                        int wd_num = Integer.parseInt(ret[3]);
                        String[] pos_kw = new String[wd_num];
                        double[] weight = new double[wd_num];
                        int p = 0;
                        for(int i = 4 ; i < 4 + wd_num * 2 ; i += 2)
                        {

                            pos_kw[p] = ret[i];
                            weight[p] = Double.parseDouble(ret[i+1]);
                            if(oid == 2945822)
                            {
                                System.out.println(pos_kw[p]);
                                System.out.println(weight[p]);
                            }
                            p++;
                        }
                        int term_len = Integer.parseInt(ret[4 + 2 * wd_num]);
                        String[] terms = new String[term_len];
                        int start = 5 + 2 * wd_num;
                        p = 0;
                        for(int i = start ; i < start + term_len ; i++)
                        {
                            terms[p] = ret[i];
                            p++;
                        }

                        tgs[oid] = new TweetGeneral(oid , pos_kw , weight , terms);

                        if(oid == 588118)
                        {
                            System.out.println("588118 kwd");
                            for(String wd : pos_kw)
                            {
                                System.out.println(wd);
                            }
                        }



                        if(!AND)
                        {
                            double spatial_score = Node.compute_dist(lat , lon , query_lat , query_lon);
                            spatial_score = 1 - spatial_score / Quadtree.MAX_SPATIAL_DIST;
                            double text_score = 0;
                            boolean flag = true;
                            for(String str : pos_kws)
                            {
                                for(int i = 0 ; i < pos_kw.length ; i++)
                                {
                                    if(pos_kw[i].equals(str))
                                    {
                                        flag = false;
                                        text_score += weight[i];
                                    }
                                }
                            }

                            if(oid == 3596989)
                            {
                                System.out.println("found");
                                System.out.println(flag);
                                for(String str : pos_kw)
                                {
                                    System.out.println(str);
                                }
                                System.out.println(alpha * spatial_score + (1 - alpha) * text_score);
                            }

                            if(flag)
                            {
                                continue;
                            }

                            if(strings_match(terms , neg_kws))
                            {
                                continue;
                            }

                            double score = alpha * spatial_score + (1 - alpha) * text_score;
                            ResultTweet rt = new ResultTweet(oid , score);
                            pq.add(rt);


                        }

                        else
                        {
                            boolean overall_flag = true;
                            for(String str : pos_kws)
                            {
                                boolean flag = false;
                                for(int i = 0 ; i < tgs[oid].get_words().length ; i++)
                                {
                                    if(tgs[oid].get_words()[i].equals(str))
                                    {
                                        flag = true;
                                    }
                                }
                                if(!flag)
                                {
                                    overall_flag = false;
                                    break;
                                }
                            }
                            if(!overall_flag)
                            {
                                continue;
                            }

                            else
                            {
                                double score = 1 -Node.compute_dist_appro(lat , lon , query_lat , query_lon) / Quadtree.MAX_SPATIAL_DIST;

                                ResultTweet rt = new ResultTweet(oid , score);
                                pq.add(rt);
                                count++;
                            }
                        }
                        //System.out.println("count = " + count);
                        //TempTest.tweets.add(t);
                    }
                }
                reader.close();
            }

            System.out.println("the total count is " + count);


            System.out.println();



        System.out.println("pq size is " + pq.size());

        for(int i = 0 ; i < 10 ; i++)
        {
            ResultTweet rt = pq.poll();
            System.out.println(rt.get_tid() + " , " + rt.get_score());
            for(String str : tgs[rt.get_tid()].get_strings())
            {
                System.out.print(str + " ,");
            }
            System.out.println();
        }
    }

    public static void indexing(String dir , int capacity , int depth , int batch_size , int dataset_size , int spatial_division) throws IOException {

        Node.current_node_id = 0;
        Node.current_depth = 0;
        FlushToDisk.master_index = 0;
        FlushToDisk.tweet_block_id = 0;
        FlushToDisk.counter = 0;

        new File(FlushToDisk.BASE).mkdir();
        FlushToDisk.ROOT = FlushToDisk.BASE + (dataset_size / 1000000) + "m" + "c" + capacity + "d" + depth + "d" + spatial_division;
        new File(FlushToDisk.ROOT).mkdir();
        FlushToDisk.TREE = ROOT + "/TreeNode";
        new File(FlushToDisk.TREE).mkdir();
        FlushToDisk.TWEETS = ROOT +"/TweetTermWeight";
        new File(FlushToDisk.TWEETS).mkdir();
        Quadtree qt = new Quadtree();
        Node.set_depth_capacity(capacity, depth);
        File f_dir = new File(dir);


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
                    int oid = Integer.parseInt(split_results[0]);
                    SpatialTweet so = new SpatialTweet(oid , Double.parseDouble(split_results[1]), Double.parseDouble(split_results[2]));
                    int num_words = Integer.parseInt(split_results[3]);
                    String[] words = new String[num_words];
                    double[] weights = new double[num_words];
                    int p = 0;
                    for(int j = 4 ; j < 4 + 2 * num_words ; j += 2)
                    {
                        words[p] = split_results[j];
                        weights[p] = Double.parseDouble(split_results[j + 1]);
                        p++;
                    }
                    int string_len = Integer.parseInt(split_results[4 + num_words * 2]);
                    String[] text = new String[string_len];
                    p = 0;

                    for(int j = 5 + num_words * 2 ; j < 5 + num_words * 2 + string_len ; j++)
                    {
                        text[p] = split_results[j];
                        p++;
                    }

                    TweetGeneral t = new TweetGeneral(oid , words , weights , text);
                    qt.insert(so); //insert the tweet into the quadtree
                    FlushToDisk.insert_tweets(t); //insert all the information about the tweet and write into disk in batching

                }
            }

            reader.close();
            System.gc();

            FlushToDisk.flush_tweets_disk(); //flush the remaining tweet data to the disk
        }

        catch (Exception e)
        {
            e.printStackTrace();
        }

        qt.build_map();
        FlushToDisk.flush_quad_tree(qt , batch_size , spatial_division);


    }

    public static double[] truncate_coordinates(String coordinates)
    {
        double[] loc = new double[2];
        String[] coor = coordinates.replaceAll("[\\[\\]]","").split(",");
        if(coor.length == 2)
        {
            loc[0] = (Double.parseDouble(coor[0]));
            loc[1] = (Double.parseDouble(coor[1]));
        }

        else
        {
            loc[0] = (Double.parseDouble(coor[1]) + Double.parseDouble(coor[3]) + Double.parseDouble(coor[5]) + Double.parseDouble(coor[7])) / 4;
            loc[1] = (Double.parseDouble(coor[0]) + Double.parseDouble(coor[2]) + Double.parseDouble(coor[4]) + Double.parseDouble(coor[6])) / 4;
        }

        return loc;
    }

    public static Object[] truncate_keywords(String[] words)
    {
        int len = words.length;
        HashMap<String , Integer> map = new HashMap<>();
        for(String s : words)
        {
            if(map.containsKey(s))
            {
                map.put(s , map.get(s) + 1);
            }

            else
            {
                map.put(s , 1);
            }
        }

        String[] uni_words = new String[map.size()];
        double[] weights = new double[map.size()];
        int i = 0;
        for(String s : map.keySet())
        {
            uni_words[i] = s;
            weights[i] = map.get(s) * 1.0 / len;
            i++;
        }

        return new Object[]{uni_words, weights};
    }


    public static boolean delete_dir(File file)
    {
        if(!file.exists())
        {
            return false;
        }

        if(file.isDirectory())
        {
            File[] files = file.listFiles();
            for(File f : files)
            {
                delete_dir(f);
            }
        }

        return file.delete();
    }

    

    static class QueryAttrRange
    {
        double lat1;
        double lat2;
        double lon1;
        double lon2;
        String[] pos_keywords;
        ArrayList<String[]> neg_phrases;
        String[] or_keywords;
        public QueryAttrRange(double lat1 ,double lat2, double lon1, double lon2, String[] pos_keywords , ArrayList<String[]> neg_phrases)
        {
            this.lat1 = lat1;
            this.lon1 = lon1;
            this.lat2 = lat2;
            this.lon2 = lon2;
            this.pos_keywords = pos_keywords;
            this.neg_phrases = neg_phrases;
        }

        public QueryAttrRange(double lat1 ,double lat2, double lon1, double lon2, String[] pos_keywords , ArrayList<String[]> neg_phrases, String[] or_keywords)
        {
            this.lat1 = lat1;
            this.lon1 = lon1;
            this.lat2 = lat2;
            this.lon2 = lon2;
            this.pos_keywords = pos_keywords;
            this.neg_phrases = neg_phrases;
            this.or_keywords = or_keywords;
        }
    }


    }


