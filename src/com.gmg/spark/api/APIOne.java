package com.gmg.spark.api;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author gmg
 * @title: APIOne
 * @projectName bigdata-Learning
 * @description: TODO
 * @date 2020/1/21 15:28
 */
public class APIOne {
    public static void main(String[] args) {
        //初始化
        SparkConf conf = new SparkConf().setAppName("JavaApiLearn").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //从hdfs文件中创建
        JavaRDD<String> textFileRDD = sc.textFile("hdfs://master:9999/users/hadoop-twq/word.txt");

        //从本地文件系统的文件中，注意file:后面肯定是至少三个///，四个也行，不能是两个
        //如果指定第二个参数的话，表示创建的RDD的最小的分区数，如果文件分块的数量大于指定的分区
        //数的话则已文件的分块数量为准
        //JavaRDD<String> textFileRDD = sc.textFile("hdfs://master:9999/users/hadoop-twq/word.txt" 2 );


        JavaRDD<String> mapRDD = textFileRDD.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                return s + "test";
            }
        });
        System.out.println("mapRDD = " + mapRDD.collect());


        //创建一个单类型的JavaRDD
        JavaRDD<Integer> integerJavaRDD = sc.parallelize(Arrays.asList(1, 2, 3, 3, 4), 2);
        System.out.println("integerJavaRDD = " + integerJavaRDD.glom().collect());

        //创建一个单类型且类型为Double的JavaRDD
        JavaDoubleRDD doubleJavaDoubleRDD = sc.parallelizeDoubles(Arrays.asList(2.0, 3.3, 5.6));
        System.out.println("doubleJavaDoubleRDD = " + doubleJavaDoubleRDD.collect());

        //创建一个key-value类型的RDD
        JavaPairRDD<String, Integer> javaPairRDD = sc.parallelizePairs(Arrays.asList(new Tuple2("test", 3), new Tuple2("kkk", 3)));
        System.out.println("javaPairRDD = " + javaPairRDD.collect());


        JavaRDD<Integer> mapRDDInte= integerJavaRDD.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer element) throws Exception {
                return element + 1;
            }
        });


        JavaRDD<User> userJavaRDD = integerJavaRDD.map(new Function<Integer, User>() {
            @Override
            public User call(Integer element) throws Exception {
                if (element < 3) {
                    return new User("小于3", 22);
                } else {
                    return new User("大于3", 23);
                }
            }
        });
        //结果：[User{userId='小于3', amount=22}, User{userId='小于3', amount=22}, User{userId='大于3', amount=23}, User{userId='大于3', amount=23}]
        System.out.println("userJavaRDD = " + userJavaRDD.collect());


        JavaRDD<Integer> flatMapJavaRDD = integerJavaRDD.flatMap(new FlatMapFunction<Integer, Integer>() {
            @Override
            public Iterator<Integer> call(Integer element) throws Exception {
                //输出一个list，这个list里的元素是0到element
                List<Integer> list = new ArrayList<>();
                int i = 0;
                while (i <= element) {
                    list.add(i);
                    i++;
                }
                return list.iterator();
            }
        });
        //结果： [0, 1, 0, 1, 2, 0, 1, 2, 3, 0, 1, 2, 3]
        System.out.println("flatMapJavaRDD = " + flatMapJavaRDD.collect());


        JavaRDD<Integer> filterJavaRDD = integerJavaRDD.filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer integer) throws Exception {
                return integer != 1;
            }
        });
        //结果为：[2, 3, 3]
        System.out.println("filterJavaRDD = " + filterJavaRDD.collect());

        JavaRDD<List<Integer>> glomRDD = integerJavaRDD.glom();
        //结果： [[1, 2], [3, 3]]， 说明integerJavaRDD有两个分区，第一个分区中有数据1和2,第二个分区中有数据3和3
        System.out.println("glomRDD = " + glomRDD.collect());


        //每一个元素加上一个初始值，而这个初始值的获取又是非常耗时的，这个时候用mapPartitions会有非常大的优势

        JavaRDD<Integer> mapPartitionTestRDD = integerJavaRDD.mapPartitions(new FlatMapFunction<Iterator<Integer>, Integer>() {
            @Override
            public Iterator<Integer> call(Iterator<Integer> integerIterator) throws Exception {
                //每一个分区获取一次初始值，integerJavaRDD有两个分区，那么会调用两次getInitNumber方法
                //所以对应需要初始化的比较耗时的操作，比如初始化数据库的连接等，一般都是用mapPartitions来为对每一个分区初始化一次，而不要去使用map操作
                Integer initNumber = getInitNumber("mapPartitions");

                List<Integer> list = new ArrayList<>();
                while (integerIterator.hasNext()) {
                    list.add(integerIterator.next() + initNumber);
                }
                return list.iterator();
            }
        });
        //结果为： [2, 3, 4, 4]
        System.out.println("mapPartitionTestRDD = " + mapPartitionTestRDD.collect());

        JavaRDD<Integer> mapInitNumberRDD = integerJavaRDD.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer integer) throws Exception {
                //遍历每一个元素的时候都会去获取初始值，这个integerJavaRDD含有4个元素，那么这个getInitNumber方法会被调用4次，严重的影响了时间，不如mapPartitions性能好
                Integer initNumber = getInitNumber("map");
                return integer + initNumber;
            }
        });
        //结果为：[2, 3, 4, 4]
        System.out.println("mapInitNumberRDD = " + mapInitNumberRDD.collect());


        // 采样
        JavaRDD<Integer> listRDD = sc.parallelize(Arrays.asList(1, 2, 3, 3), 2);

        //第一个参数为withReplacement
        //如果withReplacement=true的话表示有放回的抽样，采用泊松抽样算法实现
        //如果withReplacement=false的话表示无放回的抽样，采用伯努利抽样算法实现

        //第二个参数为：fraction，表示每一个元素被抽取为样本的概率，并不是表示需要抽取的数据量的因子
        //比如从100个数据中抽样，fraction=0.2，并不是表示需要抽取100 * 0.2 = 20个数据，
        //而是表示100个元素的被抽取为样本概率为0.2;样本的大小并不是固定的，而是服从二项分布
        //当withReplacement=true的时候fraction>=0
        //当withReplacement=false的时候 0 < fraction < 1

        //第三个参数为：reed表示生成随机数的种子，即根据这个reed为rdd的每一个分区生成一个随机种子
        JavaRDD<Integer> sampleRDD = listRDD.sample(false, 0.5, 100);



        // pipe，表示在RDD执行流中的某一步执行其他的脚本，比如python或者shell脚本等
        JavaRDD<String> dataRDD = sc.parallelize(Arrays.asList("hi", "hello", "how", "are", "you"), 2);

        //启动echo.py需要的环境变量
        Map<String, String> env = new HashMap<>();
        env.put("env", "envtest");

        List<String> commands = new ArrayList<>();
        commands.add("python");
        //如果是在真实的spark集群中，那么要求echo.py在集群的每一台机器的同一个目录下面都要有
        commands.add("/Users/tangweiqun/spark/source/spark-course/spark-rdd-java/src/main/resources/echo.py");

        JavaRDD<String> result = dataRDD.pipe(commands, env);
        //结果为： [slave1-hi-envtest, slave1-hello-envtest, slave1-how-envtest, slave1-are-envtest, slave1-you-envtest]
        System.out.println(result.collect());
    }

    //这是一个初始值获取的方法，是一个比较耗时的方法
    public static Integer getInitNumber(String source) {
        System.out.println("get init number from " + source + ", may be take much time........");
        try {
            TimeUnit.SECONDS.sleep(2);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return 1;
    }
}
