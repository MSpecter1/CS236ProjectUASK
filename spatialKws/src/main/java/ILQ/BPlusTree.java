package ILQ;

// Searching on a B+ tree in Java

import java.security.Key;
import java.util.*;

public class BPlusTree {
    int m;
    InternalBPTreeNode root;
    BPTreeLeafNode firstLeaf;
    ArrayList<BPTreeNode> nodes;
    int node_index;

    public static void main(String args[])
    {
        BPlusTree bp = new BPlusTree(2);
        for(long i = 0 ; i < 1000000 ; i++)
        {
            bp.insert(i,100);
            if(bp.search(i) == -1)
            {
                System.out.println("aaa the searhc key is " + i);
                System.exit(1);
            }
            else
            {
                //System.out.println("the null key is " + i);
            }
        }

    }

    private int binarySearch(KeyValuePair[] dps, int numPairs, long t)
    {
        Comparator<KeyValuePair> c = (o1, o2) -> {
            Long a = o1.key;
            Long b = o2.key;
            return a.compareTo(b);
        };
        return Arrays.binarySearch(dps, 0, numPairs, new KeyValuePair(t, -1), c);
    }

    // Find the leaf node
    private BPTreeLeafNode findLeafNode(long key) {

        Long[] keys = this.root.keys;
        int i;

        for (i = 0; i < this.root.degree - 1; i++) {
            if (key < keys[i]) {
                break;
            }
        }

        int child_id = this.root.childPointers[i];
        if (nodes.get(child_id) instanceof BPTreeLeafNode) {
            return (BPTreeLeafNode)nodes.get(child_id) ;
        } else {
            return findLeafNode(child_id , key);
        }
    }

    // Find the leaf node
    private BPTreeLeafNode findLeafNode(int nid, long key) {
        InternalBPTreeNode node = (InternalBPTreeNode) nodes.get(nid);
        Long[] keys = node.keys;
        int i;

        for (i = 0; i < node.degree - 1; i++) {
            if (key < keys[i]) {
                break;
            }
        }
        BPTreeNode childBPTreeNode = nodes.get(node.childPointers[i]);
        if (childBPTreeNode instanceof BPTreeLeafNode) {
            return (BPTreeLeafNode) childBPTreeNode;
        } else {
            return findLeafNode(node.childPointers[i], key);
        }
    }

    // Get the mid point
    private int getMidpoint() {
        return (int) Math.ceil((this.m + 1) / 2.0) - 1;
    }



    //returns false only if the leaf has not yet been accessed
    private boolean isEmpty() {
        return firstLeaf == null;
    }

    private static int linearNullSearch(KeyValuePair[] dps) {
        for (int i = 0; i < dps.length; i++) {
            if (dps[i] == null) {
                return i;
            }
        }
        return -1;
    }

    private static int linearNullSearch(int[] pointers) {
        for (int i = 0; i < pointers.length; i++) {
            if (pointers[i] == 0) {
                return i;
            }
        }
        return -1;
    }



    private void sortDictionary(KeyValuePair[] dictionary) {
        Arrays.sort(dictionary, (o1, o2) -> {
            if (o1 == null && o2 == null) {
                return 0;
            }
            if (o1 == null) {
                return 1;
            }
            if (o2 == null) {
                return -1;
            }
            return o1.compareTo(o2);
        });
    }

    private int[] splitChildPointers(int inid , int split) {

        InternalBPTreeNode in = (InternalBPTreeNode) nodes.get(inid);
        int[] pointers = in.childPointers;
        int[] halfPointers = new int[this.m + 1];

        for (int i = split + 1; i < pointers.length; i++) {
            halfPointers[i - split - 1] = pointers[i];
            in.removePointer(i);
        }

        return halfPointers;
    }

    private KeyValuePair[] splitDictionary(BPTreeLeafNode ln, int split) {

        KeyValuePair[] dictionary = ln.dictionary;

        KeyValuePair[] halfDict = new KeyValuePair[this.m];

        for (int i = split; i < dictionary.length; i++) {
            halfDict[i - split] = dictionary[i];
            ln.delete(i);
        }

        return halfDict;
    }

    private void splitInternalNode(int inid) {
        InternalBPTreeNode in = (InternalBPTreeNode) nodes.get(inid);
        int parentid = in.parent_id;

        int midpoint = getMidpoint();
        Long newParentKey = in.keys[midpoint];
        Long[] halfKeys = splitKeys(in.keys, midpoint);
        int[] halfPointers = splitChildPointers(inid, midpoint);

        in.degree = linearNullSearch(in.childPointers);

        InternalBPTreeNode sibling = new InternalBPTreeNode(this.m, halfKeys, halfPointers , node_index++);
        nodes.add(sibling);



        //System.out.println("in id is " + in.nid + " degree " + in.degree + " sibling id is " + sibling.nid + " degree is " + sibling.degree);

        for (int pointer : halfPointers)
        {
            if(pointer != 0)
            {
                nodes.get(pointer).parent_id = sibling.nid;
            }
        }




        sibling.rightSibling_id = in.rightSibling_id;
        if (sibling.rightSibling_id != 0) {
            ((InternalBPTreeNode)(nodes.get(sibling.rightSibling_id))).leftSibling_id = sibling.nid;
        }

        in.rightSibling_id = sibling.nid;
        sibling.leftSibling_id = in.nid;


        if (parentid == 0) {

            Long[] keys = new Long[this.m];
            keys[0] = newParentKey;
            InternalBPTreeNode newRoot = new InternalBPTreeNode(this.m, keys,node_index++);
            nodes.add(newRoot);
            newRoot.appendChildPointer(inid);
            newRoot.appendChildPointer(sibling.nid);
            this.root = newRoot;

            in.parent_id = newRoot.nid;
            sibling.parent_id = newRoot.nid;
            //TODO

        } else {
            InternalBPTreeNode parent = (InternalBPTreeNode) nodes.get(parentid);
            parent.keys[parent.degree - 1] = newParentKey;
            Arrays.sort(parent.keys, 0, parent.degree);

            int pointerIndex = parent.findIndexOfPointer(inid) + 1;
            parent.insertChildPointer(sibling.nid, pointerIndex);
            sibling.parent_id = parentid;
        }
    }

    private Long[] splitKeys(Long[] keys, int split) {

        Long[] halfKeys = new Long[this.m];

        keys[split] = null;

        for (int i = split + 1; i < keys.length; i++) {
            halfKeys[i - split - 1] = keys[i];
            keys[i] = null;
        }

        return halfKeys;
    }



    //the key is the integer representation of a node binary sequence and the value is
    public void insert(long key, int fid) {
        if (isEmpty())
        {
            BPTreeLeafNode ln = new BPTreeLeafNode(this.m, new KeyValuePair(key, fid) , node_index++);
            nodes.add(ln);
            this.firstLeaf = ln;
        }

        else
        {
            BPTreeLeafNode ln = (this.root == null) ? this.firstLeaf : findLeafNode(key);

            if (!ln.insert(new KeyValuePair(key, fid))) { //indicating the split will be invoked

                ln.dictionary[ln.numPairs] = new KeyValuePair(key, fid);
                ln.numPairs++;
                sortDictionary(ln.dictionary);


                int midpoint = getMidpoint();
                KeyValuePair[] halfDict = splitDictionary(ln, midpoint);

                if (ln.parent_id == 0)
                {
                    Long[] parent_keys = new Long[this.m];
                    parent_keys[0] = halfDict[0].key;
                    InternalBPTreeNode parent = new InternalBPTreeNode(this.m, parent_keys,node_index++);
                    nodes.add(parent);
                    ln.parent_id = parent.nid;
                    parent.appendChildPointer(ln.nid);
                }

                else
                {
                    long newParentKey = halfDict[0].key;
                    InternalBPTreeNode lnParent = (InternalBPTreeNode)nodes.get(ln.parent_id);
                    //System.out.println("new parent id is" + lnParent.nid + "parent degree is " + lnParent.degree);
                    lnParent.keys[lnParent.degree - 1] = newParentKey;
                    Arrays.sort(lnParent.keys, 0, lnParent.degree);
                }



                BPTreeLeafNode newBPTreeLeafNode = new BPTreeLeafNode(this.m, halfDict, ln.parent_id , node_index++);

                nodes.add(newBPTreeLeafNode);

                InternalBPTreeNode lnParent = (InternalBPTreeNode)nodes.get(ln.parent_id);
                int pointerIndex = lnParent.findIndexOfPointer(ln.nid) + 1;  // ln.parent.findIndexOfPointer(ln) + 1;
                lnParent.insertChildPointer(newBPTreeLeafNode.nid, pointerIndex);

                newBPTreeLeafNode.rightSibling_id = ln.rightSibling_id;
                if (newBPTreeLeafNode.rightSibling_id != 0)
                {
                    ((BPTreeLeafNode)(nodes.get(newBPTreeLeafNode.rightSibling_id))).leftSibling_id = newBPTreeLeafNode.nid;
                }
                ln.rightSibling_id = newBPTreeLeafNode.nid;
                newBPTreeLeafNode.leftSibling_id = ln.nid;

                if (this.root == null)
                {
                    this.root = lnParent;
                }

                else
                {
                    int inid = ln.parent_id;
                    while (inid != 0)
                    {
                        InternalBPTreeNode in = (InternalBPTreeNode)nodes.get(inid);
                        if (in.isOverfull())
                        {
                            splitInternalNode(inid);
                        }
                        else
                        {
                            break;
                        }
                        inid = in.parent_id;
                    }
                }

            }
        }
    }


    //the key here is the integer representation of the binary split sequence
    public int search(long key) {

        if (isEmpty()) {

            return -1;
        }

        BPTreeLeafNode ln = (this.root == null) ? this.firstLeaf : findLeafNode(key);

        KeyValuePair[] dps = ln.dictionary;
        int index = binarySearch(dps, ln.numPairs, key);



        if (index < 0) {
            System.out.println("returned because not found");
            return -1;
        } else {
            return dps[index].fid;
        }
    }


    public BPlusTree(int m) {
        this.m = m;
        this.root = null;
        this.nodes = new ArrayList<>();
        nodes.add(null);
        this.node_index = 1;
    }

    public BPlusTree(){}



    public static class BPTreeNode {
        int nid;
        int parent_id;
        public BPTreeNode(){}
    }

    public static class InternalBPTreeNode extends BPTreeNode {
        int maxDegree;
        int minDegree;
        int degree;
        int leftSibling_id;
        int rightSibling_id;
        Long[] keys;
        int[] childPointers;

        public InternalBPTreeNode(){}

        public InternalBPTreeNode(int m, Long[] keys , int id) {
            this.maxDegree = m;
            this.minDegree = (int) Math.ceil(m / 2.0);
            this.degree = 0;
            this.keys = keys;
            this.childPointers = new int[this.maxDegree + 1];
            this.leftSibling_id = 0;
            this.rightSibling_id = 0;
            this.parent_id = 0;
            this.nid = id;
        }

        public InternalBPTreeNode(int m, Long[] keys, int[] pointers, int id) {
            this.maxDegree = m;
            this.minDegree = (int) Math.ceil(m / 2.0);
            this.degree = linearNullSearch(pointers);
            this.keys = keys;
            this.childPointers = pointers;
            this.nid = id;
            this.leftSibling_id = 0;
            this.rightSibling_id = 0;
            this.parent_id = 0;
        }

        private void appendChildPointer(int pointer) {
            this.childPointers[degree] = pointer;
            this.degree++;
        }

        private int findIndexOfPointer(int pointer) {
            for (int i = 0; i < childPointers.length; i++) {
                if (childPointers[i] == pointer) {
                    return i;
                }
            }
            return -1;
        }

        private void insertChildPointer(int pointer, int index) {
            for (int i = degree - 1; i >= index; i--) {
                childPointers[i + 1] = childPointers[i];
            }
            this.childPointers[index] = pointer;
            this.degree++;
        }


        private boolean isOverfull() {
            return this.degree == maxDegree + 1;
        }



        void removePointer(int index) {
            this.childPointers[index] = 0;
            this.degree--;
        }




    }

    public static class BPTreeLeafNode extends BPTreeNode {
        int maxNumPairs;
        int minNumPairs;
        int numPairs;
        int leftSibling_id;
        int rightSibling_id;
        KeyValuePair[] dictionary;

        public BPTreeLeafNode(){}
        public BPTreeLeafNode(int m, KeyValuePair dp , int nid) {
            this.maxNumPairs = m - 1;
            this.minNumPairs = (int) (Math.ceil(m / 2) - 1);
            this.dictionary = new KeyValuePair[m];
            this.numPairs = 0;
            this.nid = nid;
            this.insert(dp);
            this.parent_id = 0;
            this.leftSibling_id = 0;
            this.rightSibling_id = 0;
        }

        public BPTreeLeafNode(int m, KeyValuePair[] dps, int parent_id , int nid) {
            this.maxNumPairs = m - 1;
            this.minNumPairs = (int) (Math.ceil(m / 2) - 1);
            this.dictionary = dps;
            this.numPairs = linearNullSearch(dps);
            this.parent_id = parent_id;
            this.nid = nid;
            this.leftSibling_id = 0;
            this.rightSibling_id = 0;

        }

        public void delete(int index) {
            this.dictionary[index] = null;
            numPairs--;
        }

        public boolean insert(KeyValuePair dp) {
            if (this.isFull()) {
                return false;
            } else {
                this.dictionary[numPairs] = dp;
                numPairs++;
                Arrays.sort(this.dictionary, 0, numPairs);
                return true;
            }
        }

        public boolean isDeficient() {
            return numPairs < minNumPairs;
        }

        public boolean isFull() {
            return numPairs == maxNumPairs;
        }

        public boolean isLendable() {
            return numPairs > minNumPairs;
        }

        public boolean isMergeable() {
            return numPairs == minNumPairs;
        }


    }

    public static class KeyValuePair implements Comparable<KeyValuePair> {
        long key; //the binary value of the morton code.
        int fid;

        public KeyValuePair() {}

        public KeyValuePair(long key, int fid) {
            this.key = key;
            this.fid = fid;
        }

        public int compareTo(KeyValuePair o) {
            return Long.compare(key, o.key);
        }
    }


}