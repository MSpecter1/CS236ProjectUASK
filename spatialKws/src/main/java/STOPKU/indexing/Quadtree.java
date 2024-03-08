package STOPKU.indexing;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Queue;

public class Quadtree {
    Node root;
    public static final double MAX_LAT = 90.0;
    public static final double MIN_LAT = -90.0;
    public static final double MAX_LON = 180.0;
    public static final double MIN_LON = -180.0;
    public static final double MAX_SPATIAL_DIST = 2.0037508342789244E7;//Node.compute_dist(MAX_LAT , MAX_LON , MIN_LAT , MIN_LON);
    public Node[] nodes_in_tree;

    public Quadtree()
    {
        Node.current_node_id = 0;
        Node.current_depth = 0;
        root = new Node(Node.current_node_id, Node.current_depth , MAX_LAT , MIN_LAT , MAX_LON , MIN_LON, true);
    }

    public Quadtree(Node root)
    {
        this.root = root;
    }

    public Node get_root()
    {
        return root;
    }

    public void insert(SpatialTweet o)
    {
        Node leaf = search_path_to_leaf(o);
        //System.out.println("the object is added to node " + current_node.get_node_id() + " the current node is a leaf ?" + current_node.is_leaf() + " size of node " + current_node.get_size());
        leaf.insert(o);
    }

    public Node search_path_to_leaf(SpatialTweet o)
    {
        Node current_node = root;
        while(!current_node.is_leaf())
        {
            Node[] child_nodes = current_node.get_child_nodes();
            for (Node child_node : child_nodes)
            {
                if (o.inside_node(child_node))
                {
                    current_node = child_node;
                    break;
                }
            }
        }
        return current_node;
    }

    public void build_map()
    {
        //System.out.println("node.current_node_id is " + Node.current_node_id);
        nodes_in_tree = new Node[Node.current_node_id + 1];
        Queue<Node> nodes = new LinkedList<>();
        nodes.add(root);
        while(!nodes.isEmpty())
        {
            Node n = nodes.remove();
            if(!n.is_leaf())
            {
                nodes.addAll(Arrays.asList(n.get_child_nodes()));
            }
            //System.out.println("node id is " + n.get_node_id());
            nodes_in_tree[n.get_node_id()] = n;
        }
    }

    public Node get_node_by_id(int id)
    {
        return nodes_in_tree[id];
    }

    public Node[] get_nodes()
    {
        return nodes_in_tree;
    }




}
