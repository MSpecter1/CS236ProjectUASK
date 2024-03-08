package ILQ;



import java.util.ArrayList;

public class ILQQuadTreeNode {

    public static int max_depth;
    public static int capacity;
    public static int current_node_id = 0;
    private int node_id;
    private double max_lon;
    private double min_lon;
    private double max_lat;
    private double min_lat;
    private boolean is_leaf;
    private String split_seq;
    private ArrayList<Tweet> objects_in_node;
    private ILQQuadTreeNode[] children_nodes;
    private int[] children_nodes_ids;
    private boolean empty;


    public ILQQuadTreeNode() {}


    public ILQQuadTreeNode(int node_id , double max_lat, double min_lat, double max_lon, double min_lon , boolean is_leaf, String split_seq)
    {
        this.node_id = node_id;
        this.max_lon = max_lon;
        this.min_lon = min_lon;
        this.max_lat = max_lat;
        this.min_lat = min_lat;
        this.objects_in_node = new ArrayList<>();
        this.is_leaf =is_leaf;
        this.split_seq = split_seq;
        children_nodes = null;
        children_nodes_ids = null;
    }

    public void clear()
    {
        this.objects_in_node = new ArrayList<>();
    }


    public void set_empty()
    {
        this.empty = (objects_in_node.size() == 0);
    }

    public boolean is_empty()
    {
        return empty;
    }

    public Object[] insert(Tweet t)
    {
        objects_in_node.add(t);

        if(get_size() > ILQQuadTreeNode.capacity && get_split_seq().length() / 2 < ILQQuadTreeNode.max_depth)
        {
            split();
            objects_in_node.clear();
            return get_child_nodes();
        }

        else
        {
            return null;
        }
    }



    public void split()
    {
        this.is_leaf = false;
        ILQQuadTreeNode c_upper_left = new ILQQuadTreeNode(++ILQQuadTreeNode.current_node_id , max_lat , min_lat + ((max_lat - min_lat) / 2) , min_lon + ((max_lon - min_lon) / 2) , min_lon , true , this.split_seq + "10");
        ILQQuadTreeNode c_upper_right = new ILQQuadTreeNode(++ILQQuadTreeNode.current_node_id ,  max_lat , min_lat + ((max_lat - min_lat) / 2) , max_lon , min_lon + ((max_lon - min_lon) / 2) , true , this.split_seq + "11");
        ILQQuadTreeNode c_lower_left = new ILQQuadTreeNode(++ILQQuadTreeNode.current_node_id ,  min_lat + ((max_lat - min_lat) / 2) , min_lat , min_lon + ((max_lon - min_lon) / 2) , min_lon , true , this.split_seq +  "00");
        ILQQuadTreeNode c_lower_right = new ILQQuadTreeNode(++ILQQuadTreeNode.current_node_id ,   min_lat + ((max_lat - min_lat) / 2) , min_lat , max_lon , min_lon + ((max_lon - min_lon) / 2) , true , this.split_seq + "01");
        this.children_nodes = new ILQQuadTreeNode[]{c_upper_left , c_upper_right , c_lower_left , c_lower_right};


        //redistribute the objects
        for(Tweet t : objects_in_node)
        {
            if(t.inside_node(c_upper_left))
            {
                c_upper_left.insert(t);
            }

            else if(t.inside_node(c_upper_right))
            {
                c_upper_right.insert(t);
            }

            else if(t.inside_node(c_lower_left))
            {
                c_lower_left.insert(t);
            }

            else
            {
                c_lower_right.insert(t);
            }
        }




    }






    public double min_dist_to_node(double lat , double lon)
    {
        if(lat <= max_lat && lat >= min_lat && lon <= max_lon && lon >= min_lon)
        {
            return 0;
        }

        double[] dist_to_corners = new double[4];
        dist_to_corners[0] = SpatialMetric.compute_dist_appro(lat , lon , max_lat , min_lon); //upper left corner
        dist_to_corners[1] = SpatialMetric.compute_dist_appro(lat , lon , max_lat , max_lon); //upper right corner
        dist_to_corners[2] = SpatialMetric.compute_dist_appro(lat , lon , min_lat , min_lon); //lower left corner
        dist_to_corners[3] = SpatialMetric.compute_dist_appro(lat , lon , min_lat , max_lon); //lower right corner

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

            double dist_to_max_lon = SpatialMetric.compute_dist_appro(lat , lon , current_lat , current_max_lon);
            double dist_to_min_lon = SpatialMetric.compute_dist_appro(lat , lon , current_lat , current_min_lon);

            while(true)
            {
                double current_mid_lon = (current_max_lon + current_min_lon) / 2;
                double dist_to_mid = SpatialMetric.compute_dist_appro(lat , lon , current_lat , current_mid_lon);

                double min_endpoint_dist = Math.min(dist_to_max_lon , dist_to_min_lon);

                if(min_endpoint_dist <= dist_to_mid) //this means the distance to the middle point is larger than one of the endpoints, in this case, we return the endpoint with the minimum distance
                {
                    return Math.min(SpatialMetric.compute_dist(lat , lon , current_lat , current_max_lon) ,  SpatialMetric.compute_dist(lat , lon , current_lat , current_min_lon));
                }

                else //entering this means the distance to the middle point is smaller than both two endpoints, which requires further binary search
                {
                    if(Math.abs(dist_to_mid - min_dist) < 0.00001)
                    {
                        return SpatialMetric.compute_dist(lat , lon , current_lat , current_mid_lon);
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

            double dist_to_max_lat = SpatialMetric.compute_dist_appro(lat , lon , current_max_lat , current_lon);
            double dist_to_min_lat = SpatialMetric.compute_dist_appro(lat , lon , current_min_lat , current_lon);
            while(true)
            {
                double current_mid_lat = (current_max_lat + current_min_lat) / 2;
                double dist_to_mid = SpatialMetric.compute_dist_appro(lat , lon , current_mid_lat , current_lon);

                double min_endpoint_dist = Math.min(dist_to_max_lat , dist_to_min_lat);

                if(min_endpoint_dist <= dist_to_mid)
                {
                    return Math.min(SpatialMetric.compute_dist(lat , lon , current_max_lat , current_lon) , SpatialMetric.compute_dist(lat , lon , current_min_lat , current_lon));
                }

                else
                {
                    if(Math.abs(dist_to_mid - min_dist) < 0.1)
                    {
                        return SpatialMetric.compute_dist(lat , lon , current_mid_lat , current_lon);
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





    public void set_children_ids(int [] child_ids)
    {
        this.children_nodes_ids = child_ids;
    }

    public int[] get_children_ids()
    {
        return children_nodes_ids;
    }

    public void set_children_nodes(ILQQuadTreeNode[] nodes)
    {
        this.children_nodes = nodes;
    }








    public static void set_depth_capacity(int capacity , int depth)
    {
        ILQQuadTreeNode.capacity = capacity;
        ILQQuadTreeNode.max_depth = depth;
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

    public ILQQuadTreeNode[] get_child_nodes()
    {
        return children_nodes;
    }

    public boolean is_leaf()
    {
        return is_leaf;
    }

    public int get_size() { return objects_in_node.size(); }


    public ArrayList<Tweet> get_node_objects() { return objects_in_node; }

    public String get_split_seq()
    {
        return split_seq;
    }

    public void set_split_seq(String new_split_seq)
    {
        this.split_seq = new_split_seq;
    }

    public int get_integer()
    {
        if(split_seq.equals(""))
        {
            return 0;
        }

        else
        {
            return Integer.parseUnsignedInt(this.split_seq ,2);
        }
    }

}
