package com.briup.chrasm_CF;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PC1 extends Configured implements Tool{
	static class pc1Mapper extends Mapper<LongWritable,Text,Text,Text>{
		@Override
		protected void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{
			context.write(new Text("1"), new Text(value.toString().split("[ ]")[1]));
		}
	}
	static class pc1Reducer extends Reducer<Text,Text,Text,NullWritable>{
		private NullWritable v = NullWritable.get();
		@Override
		protected void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException{
			int num=0;
			double p=0.0;
			double c=0.0;
			for(Text i:values){
				String[] sp = i.toString().split("[,]");
				p+=Double.parseDouble(sp[0]);
				c+=Double.parseDouble(sp[1]);
				num++;
			}
			context.write(new Text("平均准确率: "+String.format("%.2f", p/num*100)+"%    平均覆盖率: "+String.format("%.2f", c/num*100)+"%"), v);
		}
	}
	
	@Override
	public int run(String[] arg0) throws Exception {
		// 获得程序运行时的配置信息
				Configuration conf=this.getConf();
				String inputPath=conf.get("input");
				String outputPath=conf.get("output");
				// 构建新的作业
				Job job=Job.getInstance(conf,"chrasm_CF");
				job.setJarByClass(this.getClass());
				// 给job设置mapper类及map方法输出的键值类型
				job.setMapperClass(pc1Mapper.class);
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(Text.class);
				// 给job设置reducer类及reduce方法输出的键值类型
				job.setReducerClass(pc1Reducer.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(NullWritable.class);
				// 设置数据的读取方式（文本文件）及结果的输出方式（文本文件）
				job.setInputFormatClass(TextInputFormat.class);
				job.setOutputFormatClass(TextOutputFormat.class);
				// 设置输入和输出目录
				FileInputFormat.addInputPath(job,new Path(inputPath));
				FileOutputFormat.setOutputPath(job,new Path(outputPath));
				// 将作业提交集群执行
				return job.waitForCompletion(true)?0:1;
	}
	public static void main(String[] args) throws Exception {
		int status=ToolRunner.run(new PC1(),args);
		System.exit(status);
	}
}