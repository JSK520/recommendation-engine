package com.jsk.job;


import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Job7 extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Job7(), args);
	}

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		String vinpath = conf.get("vinpath");
		String jinpath = conf.get("jinpath");
		String output = conf.get("outpath");

		Job job = Job.getInstance(conf);
		job.setJarByClass(this.getClass());
		job.setJobName("CO");

		MultipleInputs.addInputPath(job, new Path(jinpath), TextInputFormat.class, Job6Mapper.class);
		MultipleInputs.addInputPath(job, new Path(vinpath), TextInputFormat.class, VMapper.class);

		TextOutputFormat.setOutputPath(job, new Path(output));

		// 判断一下输入的路径是否存在 存在就先删除
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(output)))
			fs.delete(new Path(output), true);

		job.setMapOutputKeyClass(Text.class);
		job.setReducerClass(MReducer.class);

		job.setOutputKeyClass(Text.class);
		return job.waitForCompletion(true) ? 0 : 1;

	}

	static class Job6Mapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// 10001 s20001 10 v

			String s = value.toString().trim();
			String string = s.split("[ ]")[0].trim();
			context.write(new Text(string), new Text("J" + s));

		}
	}

	static class VMapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// 10001 v20001 1
			String s = value.toString().trim();

			String string = s.split("[ ]")[0].trim();
			context.write(new Text(string), new Text("V" + s));

		}
	}

	static class MReducer extends Reducer<Text, Text, Text, NullWritable> {
		@Override
		protected void reduce(Text key, Iterable<Text> value, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			Map<String,String>map=new HashMap<>();
			List<String>vv = new ArrayList<>();
			for(Text i:value){
				if(i.toString().startsWith("J")){
					String[] split = i.toString().substring(1).split("[\t]");
					map.put(split[0], split[1]);
				}else {
					String[] split = i.toString().substring(1).split("[ ]");
					vv.add(split[0]+" "+split[1]);
				}
			}
			for(String i:vv){
				map.remove(i);
			}
			for(String i:map.keySet()){
				
				context.write(new Text(i+" "+map.get(i)), NullWritable.get());
				
			}
			/*

			String vs = "";
			String js = "";
			String[] vstr = null;
			String[] sstr = null;

			String string = value.toString().trim().replace("[", "").replace("]", "").replace(", ", ",")
					.replace("\t", " ");
			String[] split = string.split(",");
			for (int i = 0; i < split.length; i++) {
				String trim = split[i].trim();
				if (trim.split(" ")[0].equals("V")) {
					if (!vs.equals("")) {
						vs = vs + ":" + split[i].split(" ")[1] + " " + split[i].split(" ")[2] + ","
								+ split[i].split(" ")[3];
					} else {
						vs = vs + " " + split[i].split(" ")[1] + " " + split[i].split(" ")[2] + ","
								+ split[i].split(" ")[3];
					}
				} else {
					if (!js.equals("")) {
						js = js + ":" + split[i].split(" ")[1] + " " + split[i].split(" ")[2] + ","
								+ split[i].split(" ")[3];
					} else {
						js = js + " " + split[i].split(" ")[1] + " " + split[i].split(" ")[2] + ","
								+ split[i].split(" ")[3];
					}

				}
			}
			vstr = vs.split(":");
			sstr = js.split(":");
			for (String i : vstr) {
				for (String j : sstr) {
					String[] vv = i.split(" ");
					String[] jj = j.split(" ");
					String vvs = vv[0] + vv[1];
					String jjs = jj[0] + jj[1];
					if (!vvs.equals(jjs)) {
						System.out.println(j.trim());
					}
				}
			}
		*/}
	}
}

