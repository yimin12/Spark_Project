package example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Iterator;

/*
 *   @Author : Yimin Huang
 *   @Contact : hymlaucs@gmail.com
 *   @Date : 2020/11/30 15:58
 *   @Description :
 *
 */
public class GroupTestJava {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("test");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("Error");
        JavaRDD<String> lines = sc.textFile("./data/groupData.txt");
        JavaPairRDD<String, Integer> pairRDD = lines.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String line) throws Exception {
                String cls = line.split("\t")[0];
                Integer score = Integer.valueOf(line.split("\t")[1]);
                return new Tuple2<>(cls, score);
            }
        });

        JavaPairRDD<String, Iterable<Integer>> groupRDD = pairRDD.groupByKey();
        groupRDD.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Integer>> tp) throws Exception {
                /**
                 * 定长数组方式
                 */
                String cls = tp._1;
                Iterator<Integer> iter = tp._2.iterator();
                Integer[] top3 = new Integer[3];
                while(iter.hasNext()){
                    Integer next = iter.next();
                    for(int i = 0;i<top3.length;i++){
                        if(top3[i]==null){
                            top3[i] = next;
                            break;
                        }else if(next > top3[i]){
                            for(int j =2 ;j>i ;j--){
                                top3[j] = top3[j-1];
                            }
                            top3[i] = next;
                            break;
                        }
                    }
                }
                for(Integer score : top3){
                    System.out.println("cls = "+cls+",score ="+score);

                }

                /**
                 * 原生集合排序 有可能占用 Executor端的内存比较多，导致内存OOM问题
                 */
//                String cls = tp._1;
//                List<Integer> list = IteratorUtils.toList(tp._2.iterator());
//                Collections.sort(list, new Comparator<Integer>() {
//                    @Override
//                    public int compare(Integer o1, Integer o2) {
//                        return o2-o1;
//                    }
//                });
//
//                if(list.size()>3){
//                    for(int i = 0;i<3;i++){
//                        System.out.println("cls :"+cls+",score = "+list.get(i));
//                    }
//                }else{
//                    for(Integer i :list){
//                        System.out.println("cls : "+cls+",score = "+i);
//                    }
//                }

            }
        });
    }
}
