package cc.databus.algorithms.knn

import org.apache.spark.rdd.RDD

/**
  * Created by yuange.zjy on 2019/5/14.
  * Input: S, R, k, dist
  *   S: training set, format:  uid;class;f1,f2....fn
  *     uid: unique id
  *     class: labeled class
  *     f1... fn: feature values
  *   R: query set, format: uid;f1,f2...fn
  *     uid: unique id,
  *     f1..fn: feature values
  *   k: value k
  *   dist: dist function
  * Given the training set RDD with vector and l
  */
class KNNCalc[T, L](training: RDD[(T, L)]) {
  
}
