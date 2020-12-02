package example;

import java.io.Serializable;

/*
 *   @Author : Yimin Huang
 *   @Contact : hymlaucs@gmail.com
 *   @Date : 2020/11/30 17:40
 *   @Description :
 *
 */
public class SecondSort implements Comparable<SecondSort>, Serializable {

    private int first;
    private int second;

    public SecondSort(int first, int second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public int compareTo(SecondSort o) {
        if(this.first == o.first){
            return this.second - o.second;
        }
        return this.first - o.first;
    }
}
