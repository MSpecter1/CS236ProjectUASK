package STOPKU.indexing;

import java.util.*;

public class Node {
    public static int current_node_id = 0;
    public static int current_depth = 0;
    public static int max_depth;
    public static int capacity;
    private int node_id;
    private int depth;
    private double max_lon;
    private double min_lon;
    private double max_lat;
    private double min_lat;
    private boolean is_leaf;
    private int size;
    private ArrayList<Node> neighbors;
    private ArrayList<Integer> neighbor_index;
    private int[] neighbor_ids;
    private Node[] children_nodes;
    private int[] children_nodes_ids;
    private HashMap<String , TweetWeightIndexHashVal> weight_index;
    private HashMap<Integer , Integer> textual_index;
    private HashMap<Integer , SpatialTweet> id_to_spatial;
    private ArrayList<SpatialTweet> objects_in_node_arr;
    private int sub_division_num;
    private double sub_division_len;
    private HashMap<Integer, GridInNode> grid_map;
    private static final  double EARTH_RADIUS = 6378137;


    public Node(){}

    public Node(int node_id, int depth, double max_lat, double min_lat, double max_lon, double min_lon , boolean is_leaf)
    {
        this.node_id = node_id;
        this.depth = depth;
        this.max_lon = max_lon;
        this.min_lon = min_lon;
        this.max_lat = max_lat;
        this.min_lat = min_lat;
        this.is_leaf =is_leaf;
        this.size = 0;
        children_nodes = null;
        children_nodes_ids = null;
        neighbors = new ArrayList<>();
        objects_in_node_arr = new ArrayList<>();
        id_to_spatial = new HashMap<>();
        grid_map = new HashMap<>();
    }

    public Node(double max_lat, double min_lat, double max_lon, double min_lon )
    {
        this.max_lon = max_lon;
        this.min_lon = min_lon;
        this.max_lat = max_lat;
        this.min_lat = min_lat;
    }

    public void map_neigh_node_to_id()
    {
        this.neighbor_ids = new int[neighbors.size()];
        for(int i = 0 ; i < neighbors.size() ; i++)
        {
            neighbor_ids[i] = neighbors.get(i).get_node_id();
        }

        neighbors = null;
    }



    public void insert(SpatialTweet so)
    {
        objects_in_node_arr.add(so);
        size++;
        if(get_size() > Node.capacity && get_depth() <= Node.max_depth)
        {
            split();


            for(Node n : get_child_nodes())
            {
                for(Node m : get_child_nodes())
                {
                    if(n != m)
                    {
                        n.get_neighbors().add(m);
                    }
                }
            }

            if(!this.neighbors.isEmpty())
            {
                ArrayList<Node> previous_neighs = this.neighbors;
                for(Node n : previous_neighs)
                {
                    n.get_neighbors().remove(this);
                }

                for(Node n : get_child_nodes())
                {
                    for(Node previous_neigh_node : previous_neighs)
                    {
                        if(Node.is_neighbor(n , previous_neigh_node))
                        {
                            n.get_neighbors().add(previous_neigh_node);
                            previous_neigh_node.get_neighbors().add(n);
                        }
                    }
                }
            }

            this.neighbors = null;
            this.objects_in_node_arr = null;
            this.neighbor_ids = null;
            System.gc();
        }
    }



    public void split()
    {
        /*System.out.println("the splitting node is " + node_id);*/
        this.is_leaf = false;
        Node.current_depth++;
        Node c_upper_left = new Node(++Node.current_node_id , Node.current_depth , max_lat , min_lat + ((max_lat - min_lat) / 2) , min_lon + ((max_lon - min_lon) / 2) , min_lon , true);
        Node c_upper_right = new Node(++Node.current_node_id , Node.current_depth , max_lat , min_lat + ((max_lat - min_lat) / 2) , max_lon , min_lon + ((max_lon - min_lon) / 2) , true);
        Node c_lower_left = new Node(++Node.current_node_id , Node.current_depth , min_lat + ((max_lat - min_lat) / 2) , min_lat , min_lon + ((max_lon - min_lon) / 2) , min_lon , true);
        Node c_lower_right = new Node(++Node.current_node_id , Node.current_depth ,  min_lat + ((max_lat - min_lat) / 2) , min_lat , max_lon , min_lon + ((max_lon - min_lon) / 2) , true);
        this.children_nodes = new Node[]{c_upper_left , c_upper_right , c_lower_left , c_lower_right};



        //redistribute the objects
        for(SpatialTweet o : objects_in_node_arr)
        {
            if(o.inside_node(c_upper_left))
            {
                c_upper_left.insert(o);
            }

            else if(o.inside_node(c_upper_right))
            {
                c_upper_right.insert(o);
            }

            else if(o.inside_node(c_lower_left))
            {
                c_lower_left.insert(o);
            }

            else
            {
                c_lower_right.insert(o);
            }
        }



        //System.out.println("the following is the split info");
        //System.out.println(c_upper_left.get_max_lat() + " , " + c_upper_left.get_min_lat() + " , " + c_upper_left.get_max_lon() + " , " + c_upper_left.get_min_lon() + " id is " + c_upper_left.node_id + "init size " + c_upper_left.get_size());
        //System.out.println(c_upper_right.get_max_lat() + " , " + c_upper_right.get_min_lat() + " , " + c_upper_right.get_max_lon() + " , " + c_upper_right.get_min_lon() +  " id is " + c_upper_right.node_id + "init size " + c_upper_right.get_size());
        //System.out.println(c_lower_left.get_max_lat() + " , " + c_lower_left.get_min_lat() + " , " + c_lower_left.get_max_lon() + " , " + c_lower_left.get_min_lon() +  " id is " + c_lower_left.node_id + "init size " + c_lower_left.get_size());
        //System.out.println(c_lower_right.get_max_lat() + " , " + c_lower_right.get_min_lat() + " , " + c_lower_right.get_max_lon() + " , " + c_lower_right.get_min_lon() +  " id is " + c_lower_right.node_id + "init size " + c_lower_right.get_size());

    }

    public static boolean is_neighbor(Node n1 , Node n2)
    {
        return  (n1.get_max_lon() == n2.get_min_lon() && line_intersect(n1.get_max_lat() , n1.get_min_lat() , n2.get_max_lat() , n2.get_min_lat())) ||
                (n1.get_min_lon() == n2.get_max_lon() && line_intersect(n1.get_max_lat() , n1.get_min_lat() , n2.get_max_lat() , n2.get_min_lat())) ||
                (n1.get_max_lat() == n2.get_min_lat() && line_intersect(n1.get_max_lon() , n1.get_min_lon() , n2.get_max_lon() , n2.get_min_lon())) ||
                (n1.get_min_lat() == n2.get_max_lat() && line_intersect(n1.get_max_lon() , n1.get_min_lon() , n2.get_max_lon() , n2.get_min_lon()));
    }

    public static boolean line_intersect(double max1 , double min1 , double max2 , double min2)
    {
        return (min1 <= max2) && (max1 >= min2);
    }



    public void set_children_ids(int [] child_ids)
    {
        this.children_nodes_ids = child_ids;
    }

    public int[] get_children_ids()
    {
        return children_nodes_ids;
    }

    public void set_children_nodes(Node[] nodes)
    {
        this.children_nodes = nodes;
    }


    public void set_neighbor_ids(int[] neighbor_ids)
    {
        this.neighbor_ids = neighbor_ids;
    }

    public int[] get_neighbor_ids()
    {
        return neighbor_ids;
    }

    public void set_neighbors(ArrayList<Node> neigh_nodes)
    {
        this.neighbors = neigh_nodes;
    }


    public ArrayList<Node> get_neighbors()
    {
        return neighbors;
    }




    public static void set_depth_capacity(int capacity , int depth)
    {
        Node.capacity = capacity;
        Node.max_depth = depth;
    }


    private static double rad(double d){
        return d * Math.PI / 180.0;
    }


    public static double compute_dist(double lat1 , double lon1, double lat2 , double lon2) {
        /*double radLat1 = rad(lat1);
        double radLat2 = rad(lat2);
        double a = radLat1 - radLat2;
        double b = rad(lon1) - rad(lon2);
        double s = 2 *Math.asin(Math.sqrt(Math.pow(Math.sin(a/2),2)+Math.cos(radLat1)*Math.cos(radLat2)*Math.pow(Math.sin(b/2),2)));
        s = s * EARTH_RADIUS;
        return s;*/
        double ld = 69.1 * (lat1 - lat2);
        double ll = 53 * (lon1 - lon2);
        return Math.sqrt(ld * ld + ll * ll);
    }

    public static double compute_dist_appro(double lat1 , double lon1 , double lat2 , double lon2)
    {
        double ld = 69.1 * (lat1 - lat2);
        double ll = 53 * (lon1 - lon2);
        return Math.sqrt(ld * ld + ll * ll);
    }


    //this function is called to compute the minimum distance between a query point to a leaf node(a bounding rectangle)
    public static double min_dist_to_node(double lat , double lon , Node n)
    {
        double max_lat = n.get_max_lat();
        double min_lat = n.get_min_lat();
        double max_lon = n.get_max_lon();
        double min_lon = n.get_min_lon();
        if(lat <= max_lat && lat >= min_lat && lon <= max_lon && lon >= min_lon)
        {
            return 0;
        }

        double[] dist_to_corners = new double[4];
        dist_to_corners[0] = compute_dist_appro(lat , lon , max_lat , min_lon); //upper left corner
        dist_to_corners[1] = compute_dist_appro(lat , lon , max_lat , max_lon); //upper right corner
        dist_to_corners[2] = compute_dist_appro(lat , lon , min_lat , min_lon); //lower left corner
        dist_to_corners[3] = compute_dist_appro(lat , lon , min_lat , max_lon); //lower right corner

        double min_dist = Double.MAX_VALUE;
        int min_index = -1;
        for(int i = 0 ; i < 4 ; i++)
        {
            if(dist_to_corners[i] < min_dist)
            {
                min_dist = dist_to_corners[i];
                min_index = i;
            }
        }

        double second_min_val = Double.MAX_VALUE;
        int second_min_index = -1;

        for(int i = 0 ; i < 4 ; i++)
        {
            if(i != min_index)
            {
                if(dist_to_corners[i] < second_min_val)
                {
                    second_min_val = dist_to_corners[i];
                    second_min_index = i;
                }
            }
        }


        //up tp here, we know which two corners the query points has the nearest distance to


        //this indicates that the query point has minimum distance to one of the two horizontal lines
        if((min_index == 0 && second_min_index == 1) || (min_index == 1 && second_min_index == 0) || (min_index == 2 && second_min_index == 3) || (min_index == 3 && second_min_index == 2))
        {
            double current_lat;
            double current_min_lon = min_lon;
            double current_max_lon = max_lon;
            if(min_index == 0 || min_index == 1)
            {
                current_lat = max_lat;
            }

            else
            {
                current_lat = min_lat;
            }

            double dist_to_max_lon = compute_dist_appro(lat , lon , current_lat , current_max_lon);
            double dist_to_min_lon = compute_dist_appro(lat , lon , current_lat , current_min_lon);

            while(true)
            {
                double current_mid_lon = (current_max_lon + current_min_lon) / 2;
                double dist_to_mid = compute_dist_appro(lat , lon , current_lat , current_mid_lon);

                double min_endpoint_dist = Math.min(dist_to_max_lon , dist_to_min_lon);

                if(min_endpoint_dist <= dist_to_mid) //this means the distance to the middle point is larger than one of the endpoints, in this case, we return the endpoint with the minimum distance
                {
                    return Math.min(compute_dist(lat , lon , current_lat , current_max_lon) ,  compute_dist(lat , lon , current_lat , current_min_lon));
                }

                else //entering this means the distance to the middle point is smaller than both two endpoints, which requires further binary search
                {
                    if(Math.abs(dist_to_mid - min_dist) < 1)
                    {
                        return compute_dist(lat , lon , current_lat , current_mid_lon);
                    }

                    else
                    {
                        if(dist_to_mid < dist_to_max_lon)
                        {
                            current_max_lon = current_mid_lon;
                        }

                        else //indicating that dist_to_mid < dist_to_min_lon
                        {
                            current_min_lon = current_mid_lon;
                        }
                    }
                }
            }
        }

        //this indicates that the query has minimum distance to one of the two vertical lines
        else if((min_index == 0 && second_min_index == 2) || (min_index == 2 && second_min_index == 0) || (min_index == 1 && second_min_index == 3) || (min_index == 3 && second_min_index == 1))
        {
            double current_lon;
            double current_min_lat = min_lat;
            double current_max_lat = max_lat;
            if(min_index == 0 || min_index == 2)
            {
                current_lon = min_lon;
            }

            else
            {
                current_lon = max_lon;
            }

            double dist_to_max_lat = compute_dist_appro(lat , lon , current_max_lat , current_lon);
            double dist_to_min_lat = compute_dist_appro(lat , lon , current_min_lat , current_lon);
            while(true)
            {
                double current_mid_lat = (current_max_lat + current_min_lat) / 2;
                double dist_to_mid = compute_dist_appro(lat , lon , current_mid_lat , current_lon);

                double min_endpoint_dist = Math.min(dist_to_max_lat , dist_to_min_lat);

                if(min_endpoint_dist <= dist_to_mid)
                {
                    return Math.min(compute_dist(lat , lon , current_max_lat , current_lon) , compute_dist(lat , lon , current_min_lat , current_lon));
                }

                else
                {
                    if(Math.abs(dist_to_mid - min_dist) < 1)
                    {
                        return compute_dist(lat , lon , current_mid_lat , current_lon);
                    }

                    else
                    {
                        if(dist_to_mid < dist_to_max_lat)
                        {
                            current_max_lat = current_mid_lat;
                        }

                        else //indicating that dist_to_mid < dist_to_min_lon
                        {
                            current_min_lat = current_mid_lat;
                        }
                    }
                }
            }


        }

        else
        {
            System.out.println("error");
        }

        return Double.MAX_VALUE;
    }

    public void set_id_to_spatial(HashMap<Integer , SpatialTweet> id_to_spatial)
    {
        this.id_to_spatial = id_to_spatial;
    }

    public HashMap<Integer , SpatialTweet> get_id2spatial()
    {
        return id_to_spatial;
    }

    public void set_grid_map(HashMap<Integer, Node.GridInNode> grid_map)
    {
        this.grid_map = grid_map;
    }


    public HashMap<Integer , GridInNode> get_grid_map()
    {
        return grid_map;
    }

    public Object[] get_grids()
    {
        /*for(Object g: grid_map.values().toArray())
        {
            Node.GridInNode x = (Node.GridInNode)g;
            System.out.println(x.get_objs().size() + " xxxx ");
        }
        System.out.println(grid_map.values().toArray().length + " this is the len");*/
        return grid_map.values().toArray();
    }

    public PriorityQueue<GridInNode> compute_grid_pq(double lat , double lon)
    {
        PriorityQueue<GridInNode> pq = new PriorityQueue<>(Comparator.comparingDouble(o -> o.get_min_dist(lat, lon)));
        //pq.addAll(get_grids());
        //pq.addAll(Arrays.asList(get_grids()));
        for(Object n : get_grids())
        {
            pq.add((Node.GridInNode)n);
        }
        return pq;
    }

    public ArrayList<SpatialTweet> get_nodes_objects_arr()
    {
        return objects_in_node_arr;
    }

    public static class GridInNode
    {
        int gid;
        HashSet<SpatialTweet> obj_in_grid;
        double max_lat;
        double min_lat;
        double max_lon;
        double min_lon;
        public GridInNode(){}
        public GridInNode(int gid , double max_lat , double min_lat , double max_lon , double min_lon)
        {
            this.gid = gid;
            this.obj_in_grid = new HashSet<>();
            this.max_lat = max_lat;
            this.min_lat = min_lat;
            this.max_lon = max_lon;
            this.min_lon = min_lon;
        }

        public void add(SpatialTweet st)
        {
            obj_in_grid.add(st);
        }

        public HashSet<SpatialTweet> get_objs()
        {
            return obj_in_grid;
        }

        public double get_min_dist(double lat , double lon)
        {
            return Node.min_dist_to_node(lat , lon , new Node(max_lat , min_lat , max_lon , min_lon));
        }


    }


    public int get_node_id()
    {
        return node_id;
    }

    public double get_max_lat()
    {
        return max_lat;
    }

    public double get_min_lat()
    {
        return min_lat;
    }

    public double get_max_lon()
    {
        return max_lon;
    }

    public double get_min_lon()
    {
        return min_lon;
    }

    public Node[] get_child_nodes()
    {
        return children_nodes;
    }

    public boolean is_leaf()
    {
        return is_leaf;
    }

    public int get_size() { return size; }

    public void set_size(int size) { this.size = size; }

    public int get_depth() { return depth; }



    public void set_weight_index(HashMap<String , TweetWeightIndexHashVal> map)
    {
        //System.out.println("weight index set " + " the map size is " + map.size());
        this.weight_index = map;
    }

    public HashMap<String , TweetWeightIndexHashVal> get_weight_index()
    {
        return weight_index;
    }

    public void set_textual_index(HashMap<Integer , Integer> textual_index)
    {
        this.textual_index = textual_index;
    }

    public HashMap<Integer , Integer> get_textual_index()
    {
        return textual_index;
    }

    /*public void set_objects_arr(ArrayList<SpatialTweet> objs)
    {
        this.objects_in_node_arr = objs;
    }*/



    public double get_spatial_score_by_id(int id , double lat , double lon , double alpha)
    {
        SpatialTweet st = id_to_spatial.get(id);
        /*if(st == null)
        {
            System.out.println("the nid is " + this.node_id + " the tweet id is " + id);
        }*/
        if(alpha != 1)
        {
            return 1 - Node.compute_dist(lat , lon , st.get_lat() , st.get_lon()) / Quadtree.MAX_SPATIAL_DIST;
        }

        else
        {
            return 1 - Node.compute_dist_appro(lat , lon , st.get_lat() , st.get_lon()) / Quadtree.MAX_SPATIAL_DIST;
        }
    }

    public double get_appro_spatial_score_by_id(int id , double lat , double lon)
    {
        SpatialTweet st = id_to_spatial.get(id);
        return 1 - Node.compute_dist_appro(lat , lon , st.get_lat() , st.get_lon()) / Quadtree.MAX_SPATIAL_DIST;
    }


    @Override
    public int hashCode()
    {
        return new Integer(this.get_node_id()).hashCode();
    }

    @Override
    public boolean equals(Object o)
    {
        return this.get_node_id() == ((Node)(o)).get_node_id();
    }

    // code to check if tweet inside node fits inside range query
    public boolean checkRange(int id , double lat1 , double lat2, double lon1, double lon2)
    {
        SpatialTweet st = id_to_spatial.get(id);
        double tweet_lat = st.get_lat();
        double tweet_lon = st.get_lon();
        if(tweet_lat>lat1 && tweet_lat<lat2 && tweet_lon>lon1 && tweet_lon<lon2){
            return true;
        }
        else{
            return false;
        }
    }



}
