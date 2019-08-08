package com.gmg.spark.mlib.basic;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.DecisionTree;
import org.apache.spark.mllib.tree.RandomForest;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.mllib.tree.model.RandomForestModel;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
/**
 * @author gmg
 * @title: Example
 * @projectName bigdata-Learning
 * @description: TODO
 * @date 2019/8/8 16:08
 */
public class Example {
    public static void main(String[] arg){
        //生成spark对象
        SparkConf conf = new SparkConf();
        conf.set("spark.testing.memory","2147480000");  // spark的运行配置，意指占用内存2G
        JavaSparkContext sc = new JavaSparkContext("local[*]", "Spark", conf);      //第一个参数为本地模式，[*]尽可能地获取多的cpu；第二个是spark应用程序名，可以任意取;第三个为配置文件

        //训练集生成
        LabeledPoint pos = new LabeledPoint(1.0, Vectors.dense(2.0, 3.0, 3.0));//规定数据结构为LabeledPoint，1.0为类别标号，Vectors.dense(2.0, 3.0, 3.0)为特征向量
        LabeledPoint neg = new LabeledPoint(0.0, Vectors.sparse(3, new int[] {2, 1,1}, new double[] {1.0, 1.0,1.0}));//特征值稀疏时，利用sparse构建
        List l = new LinkedList();//利用List存放训练样本
        l.add(neg);
        l.add(pos);
        JavaRDD<LabeledPoint>training = sc.parallelize(l); //ＲＤＤ化，泛化类型为LabeledPoint 而不是List
        final NaiveBayesModel nb_model = NaiveBayes.train(training.rdd());

        //测试集生成
        double []  d = {1,1,2};
        Vector v =  Vectors.dense(d);//测试对象为单个vector，或者是ＲＤＤ化后的vector

        //朴素贝叶斯
        System.out.println(nb_model.predict(v));// 分类结果
        System.out.println(nb_model.predictProbabilities(v)); // 计算概率值


        //支持向量机
        int numIterations = 100;//迭代次数
        final SVMModel svm_model = SVMWithSGD.train(training.rdd(), numIterations);//构建模型
        System.out.println(svm_model.predict(v));

        //决策树
        Integer numClasses = 2;//类别数量
        Map<Integer, Integer> categoricalFeaturesInfo = new HashMap();
        String impurity = "gini";//对于分类问题，我们可以用熵entropy或Gini来表示信息的无序程度 ,对于回归问题，我们用方差(Variance)来表示无序程度，方差越大，说明数据间差异越大
        Integer maxDepth = 5;//最大树深
        Integer maxBins = 32;//最大划分数
        final DecisionTreeModel tree_model = DecisionTree.trainClassifier(training, numClasses,categoricalFeaturesInfo, impurity, maxDepth, maxBins);//构建模型
        System.out.println("决策树分类结果：");
        System.out.println(tree_model.predict(v));

        //随机森林
        Integer numTrees = 3; // Use more in practice.
        String featureSubsetStrategy = "auto"; // Let the algorithm choose.
        Integer seed = 12345;
        // Train a RandomForest model.
        final RandomForestModel forest_model = RandomForest.trainRegressor(training,
                categoricalFeaturesInfo, numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins, seed);//参数与决策数基本一致，除了seed
        System.out.println("随机森林结果：");
        System.out.println(forest_model.predict(v));
    }
}
