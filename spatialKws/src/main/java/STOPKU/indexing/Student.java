package STOPKU.indexing;

import java.util.ArrayList;

public class Student {
    int id;
    int grade;
    ArrayList<Integer> ids;
    public Student teammate;
    public Student(int id , int grade , ArrayList<Integer> ids)
    {
        this.id = id;
        this.grade = grade;
        this.ids = ids;
        teammate = null;
    }

    public Student(){}


    public void set_teammate(Student s1)
    {
        this.teammate = s1;
    }
}
