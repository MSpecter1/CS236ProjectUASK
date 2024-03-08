package ILQ;

import STOPKU.indexing.TweetTexual;

public class ILQFlushToDisk {
    public static int batch_size = 1000;
    private static TweetTexual[] tweets = new TweetTexual[batch_size];

    private static int counter = 0;
    public static final String ROOT = "ILQ";
    public static final String TWEETS = ROOT +"/TweetTextual";
    public static int master_index = 0;


    /*public static void insert_tweets(TweetTexual t) throws IOException {
        tweets[counter++] = t;
        if(counter == batch_size)
        {
            flush_tweets_disk();
            tweets = new TweetTexual[batch_size];
            counter = 0;
        }
    }

    public static void flush_tweets_disk() throws IOException {
        int currently_occupied_bytes = 0;
        String current_dir;
        for(TweetTexual tt : tweets)
        {
            if(tt == null)
            {
                break;
            }
            int num_bytes = 4 *//*tweet id*//* + 4 *//*num of words*//* ;

            for(String str : tt.get_strings())
            {
                num_bytes += (str.getBytes().length  + 4);
            }


            if(currently_occupied_bytes + num_bytes >= ByteStream.page)
            {
                current_dir = ILQFlushToDisk.TWEETS;
                page_write_buffer.writeBufferTofile(current_dir , Integer.toString(ILQFlushToDisk.master_index++));
                currently_occupied_bytes = 0;
            }

            currently_occupied_bytes += num_bytes;
            page_write_buffer.write(tt.get_tid());
            int length = tt.get_strings().length;
            page_write_buffer.write(length);

            for(int i = 0 ; i < length ; i++)
            {
                page_write_buffer.write(tt.get_strings()[i]);
            }
        }

        //write the rest of the tweets to the file
        if(page_write_buffer.get_position() != 0)
        {
            current_dir = FlushToDisk.TWEETS;
            page_write_buffer.writeBufferTofile(current_dir , Integer.toString(FlushToDisk.master_index++));
        }



    }*/

}
