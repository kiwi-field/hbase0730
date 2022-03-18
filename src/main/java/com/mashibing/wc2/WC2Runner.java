package com.mashibing.wc2;

/**
 * @author: 马士兵教育
 * @create: 2019-08-03 20:09
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


/**
 *  * wordcout
 *  * 将数据从hbase输出到hdfs
 * MR是分布式计算框架，对于数据源和数据目的地没有限制，用户可以任意选择，只不过需要实现两个类
 * InputFormat
 *          getsplits()
 *          createRecordReader()
 * OutputFormat
 *         getRecordWriter():返回值：RecordWriter
 *                                      write()
 *                                      close()
 * 注意：
 *      当需要从hbase读取数据的时候，必须使用TableMapReduceUtil.initTableMapperJob()
 *      当需要写数据到hbase的时候，必须使用 TableMapReduceUtil.initTableReduceJob()
 *              如果再代码逻辑进行实现的时候，不需要reduce，只要是向hbase写数据，那么上面的方法必须存在
 * 实现wordcount
 *  1、从hdfs读取数据
 *  2、讲数据的结果存储到hbase
 *
 *  作业：
 *  从hbase读取数据，将结果写入到hdfs
 *
 *
 *
 */
//  * 报错解决
// * https://www.jianshu.com/p/f7f78589f005
// * 在vmoption配置 -Djava.library.path=xxx\hadoop-2.6.5否则会报错
public class WC2Runner {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration(true);
        conf.set("hbase.zookeeper.quorum","node01");
        conf.set("mapreduce.app-submission.cross-platform","true");
        // mapreduce程序在本地跑
        conf.set("mapreduce.framework.name","local");

        Job job = Job.getInstance(conf);
        job.setJarByClass(WC2Runner.class);

        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);

        TableMapReduceUtil.initTableMapperJob(
                "wc",        // input table
                scan,               // Scan instance to control CF and attribute selection
                WC2Mapper.class,     // mapper class
                Text.class,         // mapper output key
                IntWritable.class,  // mapper output value
                job);
        job.setReducerClass(WC2Reducer.class);    // reducer class
        job.setNumReduceTasks(1);    // at least one, adjust as required
        FileOutputFormat.setOutputPath(job, new Path("/tmp/mr/mySummaryFile6"));  // adjust directories as required

        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("error with job!");
        }

//        //创建job对象
//        Job job = Job.getInstance(conf);
//        job.setJarByClass(WC2Runner.class);
//
//        //设置mapper类
//        job.setMapperClass(WC2Mapper.class);
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(IntWritable.class);
//
//        //设置reduce类
////        job.setReducerClass();
////        TableMapReduceUtil.initTableMapperJob();
//        // 本地跑最后一个要设置成false 集群跑设置为true
//        TableMapReduceUtil.initTableReducerJob("wc", WC2Reducer.class,job,null,null,null,null,false);
//        job.setOutputKeyClass(NullWritable.class);
//        job.setOutputValueClass(Put.class);
//
//        //指定hdfs存储数据的目录
//        FileInputFormat.addInputPath(job,new Path("/data/wc/input"));
//        job.waitForCompletion(true);
    }
}
