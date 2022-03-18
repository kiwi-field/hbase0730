package com.mashibing.wc2;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * wordcout
 * 将数据从hbase输出到hdfs
 * @author: 马士兵教育
 * @create: 2019-08-03 20:09
 */
public class WC2Mapper extends TableMapper<Text, IntWritable> {
    public static final byte[] CF = "cf".getBytes();
    public static final byte[] ATTR1 = "ct".getBytes();
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException,
            InterruptedException {
        String rowKey = null;
        Integer num = null;
        // 行键，列族和列限定符一起确定一个单元（Cell）
        for (Cell cell : value.listCells()) {
            rowKey = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
            num = Integer.valueOf(new String(value.getValue(CF, ATTR1)));
        }
        context.write(new Text(rowKey),new IntWritable(num));
    }
}
