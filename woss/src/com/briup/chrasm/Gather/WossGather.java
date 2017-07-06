package com.briup.chrasm.Gather;

//²É¼¯Ä£¿é

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.briup.util.BIDR;
import com.briup.util.Configuration;
import com.briup.woss.ConfigurationAWare;
import com.briup.woss.client.Gather;

public class WossGather implements Gather, ConfigurationAWare {

	private Properties p;
	private Configuration conf;

	@Override
	public void init(Properties p) {
		if(p==null){
			p=new Properties();
			try {
				p.load(WossGather.class.getResourceAsStream("gather.properties"));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		this.p=p;
	}

	@Override
	public void setConfiguration(Configuration conf) {
		this.conf=conf;
	}

	@Override
	public Collection<BIDR> gather() throws Exception {
		Map<String, BIDR> map = new HashMap<>();
		List<BIDR> list = new LinkedList<>();
		
		BufferedReader br = new BufferedReader(new FileReader(p.getProperty("filename","data/radwtmp_test")));
		String str;
		
		while ((str=br.readLine())!=null) {
			String[] strings = str.split("[|]");
			if(strings.length>=2){
				if("7".equals(strings[2])){
					BIDR bidr = new BIDR(
								strings[0],
								strings[1],
								new java.sql.Timestamp(Long.parseLong(strings[3])),
								null,
								strings[4],
								null
							);
					map.put(strings[4]+strings[1],bidr);
					continue;
				}
				if("8".equals(strings[2])){
					BIDR bidr = map.remove(strings[4]+strings[1]);
					bidr.setLogout_date(new java.sql.Timestamp(Long.parseLong(strings[3])));
					bidr.setTime_deration((int)(Long.parseLong(strings[3])-bidr.getLogin_date().getTime()));
					list.add(bidr);
				}
			}
		}
		br.close();
		
		return list;
	}

}
