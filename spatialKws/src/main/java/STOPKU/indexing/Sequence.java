package STOPKU.indexing;

import java.util.HashSet;

public class Sequence{
    public String[] sequence;
    public int count;
    public Sequence(String[] seq)
    {
        this.sequence = seq;
        this.count = 1;
    }


    /*@Override
    public int compareTo(Sequence x) {
        if(x.sequence.length != sequence.length)
        {
            return -1;
        }
        for(int i = 0 ; i < sequence.length ; i++)
        {
            if(!sequence[i].equals(x.sequence[i]))
            {
                return -1;
            }
        }
        return 0;
    }*/

    @Override
    public boolean equals(Object x)
    {
        Sequence o = (Sequence) x;
        if(o.sequence.length != sequence.length)
        {
            return false;
        }
        for(int i = 0 ; i < sequence.length ; i++)
        {
            if(!sequence[i].equals(o.sequence[i]))
            {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode()
    {
        int results = 0;
        StringBuilder str = new StringBuilder();
        for(int i = 0 ; i < sequence.length ; i++)
        {
            str.append(sequence[i]);
        }
        return str.toString().hashCode();
    }

    public static void main(String[] args)
    {
        Sequence s1 = new Sequence(new String[]{"a","b", "asd", "sad","asdas", "asdas", "xxxx"});
        HashSet set = new HashSet();
        set.add(s1);
        System.out.println(set.contains(s1));
        System.out.println(set.contains(new Sequence(new String[]{"a","b", "asd", "sad","asdas", "asdas", "xxxx"})));
    }


}
