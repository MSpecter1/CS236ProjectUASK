package STOPKU.indexing;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;


import java.io.*;
import java.util.ArrayList;

public class TestScript {


    /*public static void main(String[] args) throws IOException, InterruptedException {
        ArrayList<Student> students = new ArrayList<>();

        for(int i = 0 ; i < 10000 ; i++)
        {
            ArrayList<Integer> list = new ArrayList<>();
            list.add(1);
            list.add(2);
            students.add(new Student(1 , 2 ,list));
        }
        students.get(0).set_teammate(students.get(1));


        Kryo kryo = new Kryo();
        kryo.register(Student.class);
        kryo.register(ArrayList.class);


        Output output = new Output(new FileOutputStream("file.bin"));
        kryo.writeObject(output, students);
        output.close();

        Input input = new Input(new FileInputStream("file.bin"));
        ArrayList<Student> object2 =  kryo.readObject(input, ArrayList.class);

        input.close();

        //ArrayList<Student> de_students = (ArrayList<Student>) conf.asObject(bytes);


        System.out.println("deSerialization, ");
        System.exit(1);
        File f = new File("C:\\javaprojects\\spatialKws\\TreeNode\\5\\TweetTextual");
        int batch_size = (int)0.5 * ByteStream.page;
        ByteStream page_read_buffer = new ByteStream(batch_size);
        long start = System.currentTimeMillis();
        for(int j = 0 ; j < f.list().length ; j++)
        {
            File dest_file = new File(f + "\\" + j + ".txt");
            InputStream is = new FileInputStream(dest_file);
            int file_size = (int) dest_file.length();
            byte[] bytesArray = new byte[file_size];
            is.read(bytesArray , 0 , file_size);
            page_read_buffer.clear();
            page_read_buffer.write(bytesArray);
            page_read_buffer.flip();
            while(page_read_buffer.hasRemaining())
            {
                int id = page_read_buffer.readInt();
                int num_strings = page_read_buffer.readInt();
                String[] strings = new String[num_strings];
                for(int i = 0 ; i < num_strings ; i++)
                {
                    strings[i] = page_read_buffer.readString();
                }
            }
        }

        long end = System.currentTimeMillis();

        System.out.println("the total time is " + (end - start));


        *//*File f = new File("C:\\javaprojects\\spatialKws\\TreeNode\\0\\TweetTextual");
        myThread[] threads = new myThread[Runtime.getRuntime().availableProcessors()];
        int total_len = f.list().length;
        System.out.println("the total len is " + total_len);
        ByteStream[] page_read_buffers = new ByteStream[Runtime.getRuntime().availableProcessors()];
        for(int i = 0 ; i < page_read_buffers.length ; i++)
        {
            page_read_buffers[i] = new ByteStream(2 * ByteStream.page);
        }

        int batch = total_len / threads.length;
        long start1 = System.currentTimeMillis();
        for(int i = 0 ; i < threads.length ; i++)
        {
            threads[i] = new myThread(i * batch, (i+1) * batch , page_read_buffers[i]);
            threads[i].start();
        }

        for(myThread t : threads)
        {
            t.join();
        }
        long end1 = System.currentTimeMillis();
        System.out.println("the total time in multi-threading is " + (end1 - start1));*//*
    }


     static class myThread extends Thread
    {
        ByteStream page_read_buffer;
        int start;
        int end;
        public myThread(int start , int end , ByteStream buffer)
        {
            this.start = start;
            this.end = end;
            this.page_read_buffer = buffer;
        }

        public void run()
        {
            try
            {
                for(int j = start ; j < end ; j++)
                {
                    File f = new File("C:\\javaprojects\\spatialKws\\TreeNode\\0\\TweetTextual");
                    File dest_file = new File(f + "\\" + j + ".txt");
                    InputStream is = new FileInputStream(dest_file);
                    int file_size = (int) dest_file.length();
                    byte[] bytesArray = new byte[file_size];
                    is.read(bytesArray , 0 , file_size);
                    page_read_buffer.clear();
                    page_read_buffer.write(bytesArray);
                    page_read_buffer.flip();
                    while(page_read_buffer.hasRemaining())
                    {
                        int id = page_read_buffer.readInt();
                        int num_strings = page_read_buffer.readInt();
                        String[] strings = new String[num_strings];
                        for(int i = 0 ; i < num_strings ; i++)
                        {
                            strings[i] = page_read_buffer.readString();
                        }
                    }
                }
            }

            catch (Exception e)
            {
                e.printStackTrace();
            }
        }
    }*/
}
