package com.jsk.job;


import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;

import org.apache.hadoop.io.Text;
import org.omg.CORBA.portable.ValueInputStream;

public class Test {
/*
 * 10001	[20001, 20005, 20006, 20007, 20002]
10002	[20006, 20003, 20004]
10003	[20002, 20007]
10004	[20001, 20002, 20005, 20006]
10005	[20001]
10006	[20004, 20007]
 */
	public static void main(String[] args) throws Exception {
		
//		J 10001 20001	10 V 10001 20001 1
		String value = "[V 10001 20001 1, V 10001 20005 1, V 10001 20006 1, V 10001 20007 1, V 10001 20002 1, J 10001 20001	10, J 10001 20002	11, J 10001 20003	1, J 10001 20004	2, J 10001 20005	9, J 10001 20006	10, J 10001 20007	8]";
		String vs ="";
		String js ="";
		String[] vstr = null;
		String[] sstr = null;
		String string = value.toString().substring(1).trim().replace("[", "").replace("]", "").replace(", ", ",").replace("\t", " ");
		//得到：V 10001 20001 1, V 10001 20005 1, V 10001 20006 1, V 10001 20007 1, V 10001 20002 1, J 10001 20001	10, J 10001 20002	11, J 10001 20003	1, J 10001 20004	2, J 10001 20005	9, J 10001 20006	10, J 10001 20007	8
		System.out.println(string);
		String[] split = string.split(",");
		
		for (int i = 0; i < split.length; i++) {
			String trim = split[i].trim();
			if(trim.split(" ")[0].equals("V")){
				if(!vs.equals("")){
				vs = vs+":"+split[i].split(" ")[1]+" "+split[i].split(" ")[2]+","+split[i].split(" ")[3];
				}else{
					vs = vs+" "+split[i].split(" ")[1]+" "+split[i].split(" ")[2]+","+split[i].split(" ")[3];
				}
			} else{
				if(!js.equals("")){
					js = js+":"+split[i].split(" ")[1]+" "+split[i].split(" ")[2]+","+split[i].split(" ")[3];
					}else{
						js = js+" "+split[i].split(" ")[1]+" "+split[i].split(" ")[2]+","+split[i].split(" ")[3];
					}
			
			}
		}
		vstr = vs.split(":");
		sstr = js.split(":");
		for (String i : vstr) {
			for (String j : sstr) {
				String[] vv = i.split(" ");
				String[] jj = j.split(" ");
				String vvs = vv[0]+vv[1];
				String jjs = jj[0]+jj[1];
				if(!vvs.equals(jjs)){
					System.out.println(j.trim());
				}
			}
		}
	
		if(!js.split("\t")[0].equals((vs.split(" ")[0]+" "+vs.split(" ")[1]))){
//			System.out.println(js.split("\t")[0]+"\t"+js.split("\t")[1]);
		}
	
		
		
		/*BufferedReader br = new BufferedReader(new InputStreamReader( new FileInputStream("/home/chx/part-r-00000")));
		String value="";
		while(true){
			value = br.readLine();
			if(value==null)break;*/
//			System.out.println(value);
			
//			String[] ss = value.split("[\t]");
//			String data = ss[1].substring(4, ss[1].length()-1);
//			data = data.replace(",", "");
//			System.out.println(data);
//			
//			
//		}
//			
//			String vStr = "";
//			String sStr = "";
//			String a ="";
//			String string = value.trim().replace(",", "");
//			int v = string.indexOf("V");
//			int s = string.indexOf("S");
//			if(v>s){
//				
//			 vStr = string.substring(s+1, v).trim();
//			 sStr =  string.substring(v+1, string.length()-1).trim();
//			}else{
//			
//				 vStr = string.substring(v+1, s).trim();
//				 sStr =  string.substring(s+1, string.length()-1).trim();
//			}
//			/*
//			 * 10004:1 10001:1 10005:1   向量
//			 *	20007:1 20001:3 20002:2 20005:2 20006:2  相似度
//			 */
//			String[] vstrs = vStr.split(" ");
//			String[] sstrs = sStr.split(" ");
//			for (int i = 0; i < sstrs.length; i++) {
//				for (int j = 0; j < vstrs.length; j++) {
//					String vv =vstrs[j].trim().split(":")[0];
//					String sv = sstrs[i].trim().split(":")[0];
//				
//					a =vv;
//					int vb = Integer.parseInt(sstrs[i].split(":")[1]);
//					int sb = Integer.parseInt(vstrs[j].split(":")[1]);
//					int b =vb*sb;
////					context.write(new Text(a), new IntWritable(b));
//					System.out.println(vv+","+sv+"      "+b);
//				}
//			}
//			
//			
			
			
			
			
			
			
			
			
//			
//			
//			
		/*	String[] ss = value.split("[\t]");
			String data = ss[1].substring(4, ss[1].length()-1);
			data = data.replace(",", "");
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
//					context.write(new Text(i.split("[:]")[0]+","+split[0]), new Text(split[1]));
					System.out.println(i.split("[:]")[0]+","+split[0]+"      "+split[1]);
				}
			
		}
		}
	
		*/
//		
//				String value = "20007	[V, 10003:1, 10001:1, 10006:1, S, 20001:1, 20002:2, 20004:1, 20005:1, 20006:1, 20007:3]";
//				String str= value.toString().replace(",", "");
//				int v = str.indexOf("V");
//				int s = str.indexOf("S");
//				String vStr = str.substring(v+1, s).trim();
//				String sStr = str.substring(s+1,str.length()-1).trim();
//				System.out.println(vStr);
//				System.out.println(sStr);
//				String[] vstrs = vStr.split(" ");
//				String[] sstrs = sStr.split(" ");
//				
//				for (int i = 0; i < vstrs.length; i++) {
//					for (int j = 0; j < sstrs.length; j++) {
//						String a = vstrs[i].split(":")[0]+","+sstrs[j].split(":")[0];
//						int vb = Integer.parseInt(vstrs[i].split(":")[1]);
//						int sb = Integer.parseInt(sstrs[j].split(":")[1]);
//						int b =vb*sb;
//						System.out.println(a+"\t"+b);
//					}
//				}
	}
	
}

