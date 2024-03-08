package STOPKU.indexing;

import STOPKU.disk.FlushToDisk;
import com.aliasi.tokenizer.EnglishStopTokenizerFactory;
import com.aliasi.tokenizer.IndoEuropeanTokenizerFactory;
import com.aliasi.tokenizer.TokenizerFactory;
import org.json.JSONObject;

import java.io.*;
import java.util.*;
import java.util.zip.GZIPInputStream;

public class Preprocess {
        public static int oid = 0;
    public static void main(String[] args) throws IOException {
        HashSet<String> dict = new HashSet<>();
        BufferedReader readert = new BufferedReader(new FileReader("words.txt"));
        String linex;
        while((linex = readert.readLine()) != null && !linex.equals(""))
        {
            String word = linex.split(" ")[0];
            dict.add(word);
        }

        readert.close();

        HashSet<String> stopdict = new HashSet<>();
        readert = new BufferedReader(new FileReader("stopwords_en.txt"));
        while((linex = readert.readLine()) != null && !linex.equals(""))
        {
            String word = linex.split(" ")[0];
            stopdict.add(word);
        }
        readert.close();


        ArrayList<Integer> list = new ArrayList<>();
        HashSet<Sequence> sequence_set1 = new HashSet<>();
        HashSet<Sequence> sequence_set2 = new HashSet<>();
        HashSet<Sequence> sequence_set3 = new HashSet<>();
        HashSet<Sequence> sequence_set4 = new HashSet<>();
        HashSet<Sequence> sequence_set5 = new HashSet<>();
        ArrayList<HashSet<Sequence>> list_of_sets = new ArrayList<>();
        list_of_sets.add(sequence_set1);
        list_of_sets.add(sequence_set2);
        list_of_sets.add(sequence_set3);
        list_of_sets.add(sequence_set4);
        list_of_sets.add(sequence_set5);
        int total_count = 0;
        //list.add(10000);
        //list.add(30000);
        /*for(int i = 2000000 ; i <= 10000000 ; i += 2000000)
        {
            list.add(i);
        }*/

        for(int i = 30000000 ; i <= 100000000 ; i += 20000000)
        {
            list.add(i);
        }

        int batch = 100000;

        for(int count : list)
        {
            oid = 0;
            int current_count = 0;
            ArrayList<SpatialTweet> sts = new ArrayList<>();
            ArrayList<TweetGeneral> tgs = new ArrayList<>();

            String dir = "pdata/tweet" + count ;
            File f_dir = new File(dir);
            TokenizerFactory factory = IndoEuropeanTokenizerFactory.INSTANCE;
            factory = new EnglishStopTokenizerFactory(factory);
            HashMap<Sequence , Integer> sequence_map = new HashMap<>();

            HashMap<Sequence , Integer> sequence2_map = new HashMap<>();
            HashMap<Sequence , Integer> sequence3_map = new HashMap<>();
            HashMap<Sequence , Integer> sequence4_map = new HashMap<>();
            HashMap<Sequence , Integer> sequence5_map = new HashMap<>();

            boolean flag = false;
            for(String subfile : f_dir.list())
            {
                BufferedReader reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(f_dir + "/" + subfile))));
                String line;
                while((line = reader.readLine()) != null && !line.equals(""))
                {
                    Object[] ret = extract_data(line , factory , dict , stopdict);
                    if(ret != null)
                    {
                        SpatialTweet so = (SpatialTweet) ret[0];
                        TweetGeneral t = (TweetGeneral) ret[1];
                        place(sequence_map , t , 1 , list_of_sets.get(0));
                        place(sequence2_map , t , 2 , list_of_sets.get(1));
                        place(sequence3_map , t,  3 , list_of_sets.get(2));
                        place(sequence4_map ,t , 4 , list_of_sets.get(3));
                        place(sequence5_map ,t , 5 , list_of_sets.get(4));
                        place(sequence_map , t , 6 , list_of_sets.get(0));
                        total_count ++;
                        System.out.println("the total count is " + total_count + "size is" +  sequence5_map.size() +  "set size is " + list_of_sets.get(4).size());

                        sts.add(so);
                        tgs.add(t);
                        if(sts.size() == batch)
                        {
                            String dire = "pdata/" + "tweet" + count;
                            File f = new File(dire);
                            if(!f.exists())
                            {
                                f.mkdir();
                            }
                            write(sts , tgs , dire, current_count);
                            current_count ++;
                        }



                        if(current_count * batch == count)
                        {
                            flag = true;
                            break;
                        }


                    }
                }



                if(flag)
                {
                    break;
                }
            }

            write(sequence_map , 1 ,  ""+count);
            write(sequence2_map , 2 , ""+ count);
            write(sequence3_map , 3 , ""+count);
            write(sequence4_map , 4 , "" + count);
            write(sequence5_map , 5 , "" + count);
            write(sequence5_map , 6 , "" + count);
            System.gc();
        }
    }

    public static void place(HashMap<Sequence , Integer> map , TweetGeneral t , int len , HashSet<Sequence> set)
    {
        for(int i = 0 ; i <= t.get_words().length - len; i++)
        {
            String[] strs = new String[len];
            for(int p = 0 ; p < len ; p++)
            {
                strs[p] = t.get_words()[i + p];
            }
            Sequence s = new Sequence(strs);
            if(map.containsKey(s))
            {
                if(map.get(s) == 1)
                {
                    set.remove(s);
                }
                map.put(s , map.get(s) + 1);
            }

            else
            {
                if(map.size() < 100000)
                {
                    map.put(s , 1);
                    set.add(s);
                }

                else
                {
                    if(!set.isEmpty())
                    {
                        Sequence ss = set.iterator().next();
                        set.remove(ss);
                        map.remove(ss);
                        map.put(s , 1);
                        set.add(s);
                    }

                }
            }
        }
    }

    static class Temp
    {
        String[] ss;
        int freq;
        public Temp(String[] sss , int freqq)
        {
            ss = sss;
            freq = freqq;
        }
    }

    public static void write(HashMap<Sequence , Integer> map , int len , String dir) throws IOException {
        ArrayList<Temp> ss = new ArrayList<>();

        for(Sequence s : map.keySet())
        {
            if(map.get(s) == null)
            {
                continue;
            }
            ss.add(new Temp(s.sequence , map.get(s)));
        }
        ss.sort((o1, o2) -> Integer.compare(o2.freq, o1.freq));
        File f = new File("pdata/" +  "kws" + dir);
        if(!f.exists())
        {
            f.mkdir();
        }
        BufferedWriter out = new BufferedWriter(new FileWriter(f + "/" + len + ".txt"));
        int count = 0;

        for(Temp t : ss)
        {
            String[] x = t.ss;
            double lat = -90.0 + 180 * Math.random();
            double lon = -180.0 + 360 * Math.random();
            out.write(lat+"");
            out.write(" ");
            out.write(lon+"");
            out.write(" ");
            for(String str : x)
            {
                out.write(str);
                out.write(" ");
            }
            out.write(t.freq + "");
            System.out.println("the written frequency value is " + t.freq);
            out.write("\n");
            count ++;
            if(count == 10000)
            {
                break;
            }
            map = null;
            ss = null;
        }
        out.close();
        System.out.println("-----------------------");

    }

    public static void write(ArrayList<SpatialTweet> sts , ArrayList<TweetGeneral> tgs , String det , int current_count) throws IOException {

        File f = new File(det + "/" + current_count + ".txt");

        BufferedWriter out = new BufferedWriter(new FileWriter(det + "/" + current_count + ".txt"));
        for(int i = 0 ; i < sts.size() ; i++)
        {
            out.write(sts.get(i).get_oid() + "");
            out.write(" ");
            out.write(sts.get(i).get_lat() + "");
            out.write(" ");
            out.write(sts.get(i).get_lon() + "");
            out.write(" ");
            out.write(tgs.get(i).get_words().length + "");
            out.write(" ");
            for(int j = 0 ; j < tgs.get(i).get_words().length ; j++)
            {
                out.write(tgs.get(i).get_words()[j]);
                out.write(" ");
                out.write(tgs.get(i).get_weights()[j] + "");
                out.write(" ");
            }
            out.write(tgs.get(i).get_weights().length + "");
            out.write(" ");
            for(int j = 0 ; j < tgs.get(i).get_words().length ; j++)
            {
                //System.out.println("the get_word is " + tgs.get(i).get_words()[j]);
                out.write(tgs.get(i).get_words()[j]);
                out.write(" ");
            }
            out.write("\n");
        }
        out.close();
        sts.clear();
        sts = new ArrayList<SpatialTweet>();
        tgs.clear();
        tgs = new ArrayList<TweetGeneral>();
    }

    public static Object[] extract_data(String s, TokenizerFactory factory , HashSet<String> dic , HashSet<String> stopws)
    {
        JSONObject jo = new JSONObject(s);
        if(jo.has("text") && jo.get("lang").equals("en"))
        {
            //System.out.println("original " + (String)(jo.get("text")));
            String temp = ((String)(jo.get("text"))).toLowerCase();
            //System.out.println(temp);
            double[] loc;
            if(!jo.isNull("geo"))
            {
                loc = truncate_coordinates((((JSONObject)(jo.get("geo"))).get("coordinates")).toString());
            }
            else
            {
                loc = truncate_coordinates((((JSONObject)(((JSONObject)jo.get("place")).get("bounding_box"))).get("coordinates")).toString());
            }
            String[] strings = factory.tokenizer(temp.toCharArray(), 0 , temp.length()).tokenize();
            ArrayList<String> filtered = new ArrayList<>();

            for(String str : strings)
            {
                if(!str.equals("") && dic.contains(str) && !stopws.contains(str))
                {
                    filtered.add(str);
                }
            }


            strings = new String[filtered.size()];
            for(int i = 0 ; i < filtered.size() ; i++)
            {
                strings[i] = filtered.get(i);
            }
            Object[] textual = truncate_keywords(strings);
            String[] words = (String[]) textual[0];
            double[] weights = (double[]) textual[1];
            SpatialTweet so = new SpatialTweet(oid , loc[0], loc[1]);
            TweetGeneral t = new TweetGeneral(oid++, words, weights, strings);

            return new Object[]{so, t};


        }

        else
        {
            return null;
        }

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
}
