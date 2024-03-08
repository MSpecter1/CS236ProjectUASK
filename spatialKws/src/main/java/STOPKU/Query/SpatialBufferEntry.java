package STOPKU.Query;

import STOPKU.indexing.Node;
import STOPKU.indexing.SpatialTweet;


import java.util.HashMap;

public class SpatialBufferEntry {
    HashMap<Integer , SpatialTweet> id_to_spatial;
    HashMap<Integer, Node.GridInNode> grid_map;
    int hit;
    long size;
    public SpatialBufferEntry(HashMap<Integer , SpatialTweet> id_to_spatial  , HashMap<Integer, Node.GridInNode> grid_map , long size)
    {
        this.id_to_spatial = id_to_spatial;
        this.grid_map = grid_map;
        this.hit = 0;
        this.size = size;
    }

    public SpatialBufferEntry(HashMap<Integer , SpatialTweet> id_to_spatial  , long size)
    {
        this.id_to_spatial = id_to_spatial;
        this.grid_map = grid_map;
        this.hit = 0;
        this.size = size;
    }


    public HashMap<Integer , SpatialTweet> get_id_to_spatial_map()
    {
        return id_to_spatial;
    }

    public HashMap<Integer , Node.GridInNode> get_grid_map()
    {
        return grid_map;
    }

    public long get_size() {return size; }

    public int get_hit()
    {
        return hit;
    }

    public void increment_hit() {hit += 1;}
}
