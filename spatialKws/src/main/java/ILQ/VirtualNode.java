package ILQ;

import java.util.HashMap;

public class VirtualNode {
    /*String split_sequence;
    private double max_lat;
    private double max_lon;
    private double min_lat;
    private double min_lon;
    VirtualNode child_virtual_node[];
    private static final double EARTH_RADIUS = 6378137;


    public VirtualNode(double max_lat , double min_lat , double max_lon , double min_lon , String split_sequence)
    {
        this.split_sequence = split_sequence;
    }

    public double min_dist_to_node(double lat , double lon)
    {
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
                    if(Math.abs(dist_to_mid - min_dist) < 0.00001)
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
                    if(Math.abs(dist_to_mid - min_dist) < 0.00001)
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

    private static double rad(double d){
        return d * Math.PI / 180.0;
    }



    public static double compute_dist_appro(double lat1 , double lon1 , double lat2 , double lon2)
    {
        double ld = 69.1 * (lat1 - lat2);
        double ll = 53 * (lon1 - lon2);
        return Math.sqrt(ld * ld + ll * ll);
    }

    public String get_split_sequence()
    {
        return split_sequence;
    }

    public VirtualNode[] get_child_node()
    {
        VirtualNode vn_ul = new VirtualNode(max_lat , min_lat + ((max_lat - min_lat) / 2) , min_lon + ((max_lon - min_lon) / 2) , min_lon ,get_split_sequence() + "10");
        VirtualNode vn_ur = new VirtualNode(max_lat , min_lat + ((max_lat - min_lat) / 2) , max_lon , min_lon + ((max_lon - min_lon) / 2) , get_split_sequence() + "11");
        VirtualNode vn_ll = new VirtualNode(min_lat + ((max_lat - min_lat) / 2) , min_lat , min_lon + ((max_lon - min_lon) / 2) , min_lon ,get_split_sequence() + "00");
        VirtualNode vn_lr = new VirtualNode( min_lat + ((max_lat - min_lat) / 2) , min_lat , max_lon , min_lon + ((max_lon - min_lon) / 2) ,get_split_sequence() + "01");

        return new VirtualNode[]{vn_ul , vn_ur , vn_ll , vn_lr};

    }


    public double compute_score(HashMap<String , ILQQuadTree> word_quadtree_map, HashMap<String , BPlusTree> word_btree_map , String[] pos_keywords , double lat , double lon , double alpha)
    {
        double spatial_score = alpha * compute_spatial_score(lat , lon);
        //double texutual_score = (1 - alpha) * compute_textual_score()
        return spatial_score;
    }

    public double compute_spatial_score(double lat , double lon)
    {
        double min_dist = this.min_dist_to_node(lat , lon);
        double spatial_score = 1 - min_dist / ILQQuadTree.MAX_SPATIAL_DIST;
        return spatial_score;
    }

    public double compute_textual_score(HashMap<String , ILQQuadTree> word_quadtree_map, HashMap<String , BPlusTree> word_btree_map , String[] pos_keywords)
    {
        return 0;
    }*/




}

