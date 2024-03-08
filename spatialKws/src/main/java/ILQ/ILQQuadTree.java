package ILQ;



import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;


public class ILQQuadTree {

    public String word;
    ILQQuadTreeNode root;
    private int max_split_len;


    public ILQQuadTree(){}

    public ILQQuadTree(String word)
    {
        this.word = word;
        this.max_split_len = 0;
        root = new ILQQuadTreeNode(ILQQuadTreeNode.current_node_id , SpatialMetric.MAX_LAT , SpatialMetric.MIN_LAT , SpatialMetric.MAX_LON , SpatialMetric.MIN_LON, true , "");

    }

    public ILQQuadTree(ILQQuadTreeNode root)
    {
        this.root = root;
    }

    public ILQQuadTreeNode get_root()
    {
        return root;
    }

    public void insert(Tweet t)
    {
        ILQQuadTreeNode leaf = search_path_to_leaf(t);
        //System.out.println("the object is added to node " + current_node.get_node_id() + " the current node is a leaf ?" + current_node.is_leaf() + " size of node " + current_node.get_size());
        Object[] ret = leaf.insert(t);
        if(ret != null)
        {
            ILQQuadTreeNode[] new_leafs = (ILQQuadTreeNode[]) ret;
            for(ILQQuadTreeNode n : new_leafs)
            {
                if(n.get_split_seq().length() > this.max_split_len)
                {
                    max_split_len = n.get_split_seq().length();
                }
            }
        }
    }

    public ILQQuadTreeNode search_path_to_leaf(Tweet t)
    {
        ILQQuadTreeNode current_node = root;
        while(!current_node.is_leaf())
        {
            ILQQuadTreeNode[] child_nodes = current_node.get_child_nodes();
            for (ILQQuadTreeNode child_node : child_nodes)
            {
                if (t.inside_node(child_node))
                {
                    current_node = child_node;
                    break;
                }
            }
        }
        return current_node;
    }


    public ArrayList<ILQQuadTreeNode> get_leaf_nodes()
    {
        LinkedList<ILQQuadTreeNode> nodes = new LinkedList<>();
        nodes.add(root);
        ArrayList<ILQQuadTreeNode> leaf_nodes = new ArrayList<>();
        while(!nodes.isEmpty())
        {
            ILQQuadTreeNode node = nodes.remove();
            if(node.is_leaf() && !node.is_empty())
            {
                leaf_nodes.add(node);
            }
            if(node.get_child_nodes()!=null)
            {
                Collections.addAll(nodes, node.get_child_nodes());
            }
        }
        return leaf_nodes;
    }

    public boolean is_sequence_black_leaf(String seq)
    {
        ILQQuadTreeNode current_node = get_root();

        if(current_node.is_leaf()) //in this case, the root node is the only node in the quadtree and it is the subset of any split sequence
        {
            return true;
        }

        if(seq.equals(""))
        {
            return false;
        }

        else
        {
            int index = 0;
            while(index < seq.length())
            {
                String split_direction = seq.substring(index , index+2);
                index += 2;

                if(split_direction.equals("10"))
                {
                    current_node = current_node.get_child_nodes()[0];
                }

                else if(split_direction.equals("11"))
                {
                    current_node = current_node.get_child_nodes()[1];
                }

                else if(split_direction.equals("00"))
                {
                    current_node = current_node.get_child_nodes()[2];
                }

                else if(split_direction.equals("01"))
                {
                    current_node = current_node.get_child_nodes()[3];
                }


                if(current_node.is_leaf() && current_node.get_size() > 0)
                {
                    return true;
                }

            }

            //if this is reached, that means we have traversed the split sequence of the virtual node but has not yet found a black leaf node.
            return false;
        }


    }


}
