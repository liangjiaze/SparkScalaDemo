package cn.jade.wordCount

import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileUtil
object hdfs_connect extends App {
  val output = new Path("hdfs://node1:9000/emp_data/");
  val hdfs = org.apache.hadoop.fs.FileSystem.get(
    new java.net.URI("hdfs://node1:9000"), new org.apache.hadoop.conf.Configuration())
  // 删除输出目录
//  if (hdfs.exists(output)) hdfs.delete(output, true)
  //遍历目录
  val fs= hdfs.listStatus(output)
  val listPath = FileUtil.stat2Paths(fs)
  for(p<-listPath) println(p)
}
