package com.jsk.job;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.jboss.netty.util.EstimatableObjectWrapper;

public class Job6 extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Job6(), args);
	}
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		String inpath = conf.get("inpath");
		String outpath = conf.get("outpath");
		//构建job
		Job job = Job.getInstance(conf);
		
		//指定一个完整MapRed所在类以及相关信息
		job.setJarByClass(Job6.class);
		job.setJobName("Job6");
		//指定Map程序所在的类以及keyout valueout类型
		job.setMapperClass(SumMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		//指定reduce程序所在的类以及keyout valueout类型IntWritable
		job.setReducerClass(SumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		//判断一下输入的路径是否存在  存在就先删除
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(outpath)))
			fs.delete(new Path(outpath), true);
		
		//指定数据来源和数据去向相关的输入输出流以及文件位置
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(job, new Path(inpath));
		TextOutputFormat.setOutputPath(job, new Path(outpath));
		//提交运行任务
		
		return job.waitForCompletion(true)?0:1;
	}
	static class SumMapper extends Mapper<LongWritable, Text, Text,Text>{
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
//		20001	[V, 10004:1, 10001:1, 10005:1, S, 20007:1, 20001:3, 20002:2, 20005:2, 20006:2]
			
			String[] ss = value.toString().split("[\t]");
			String data = ss[1].substring(4, ss[1].length()-1);
			data = data.replace(", ", " ");
			String[] dStrings  = null;
			
			String[] v = null;
			String[] s = null;
			if(data.contains("V")){
				dStrings = data.split("[V]");
			    s=dStrings[0].trim().split("[ ]");
			    v=dStrings[1].trim().split("[ ]");
			}else{ 
				dStrings = data.split("[S]");
				 s=dStrings[1].trim().split("[ ]");
				 v=dStrings[0].trim().split("[ ]");
			}
			for(String i:v){
				for(String j:s){
					i.split("[:]");
					String[] split = j.split("[:]");
					context.write(new Text(i.split("[:]")[0]+" "+split[0]), new Text(split[1]));
				}
			}
			

		}
	}
	static class SumReducer extends Reducer<Text, Text, Text,  Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> value, Reducer<Text, Text, Text,  Text>.Context context)
				throws IOException, InterruptedException {
			
			int count = 0;
			for (Text i : value) {
				count +=  Integer.parseInt(i.toString());
			}
			context.write(key, new Text(count+""));
		}
	}
}
