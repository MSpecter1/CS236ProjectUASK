package STOPKU.indexing;

public class TweetTexualIndex {
    private int id;
    private int file;

    public TweetTexualIndex() {}
    public TweetTexualIndex(int id , int file)
    {
        this.id = id;
        this.file = file;
    }

    public int get_id()
    {
        return id;
    }

    public int get_file()
    {
        return file;
    }
}

