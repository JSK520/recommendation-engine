package WossGather;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.briup.util.BIDR;
import com.briup.util.Configuration;
import com.briup.woss.ConfigurationAWare;
import com.briup.woss.client.Gather;

import WossConfiguration.WossConfiguration;

public class WossGather implements Gather, ConfigurationAWare {

	private Configuration conf;
	private Properties p;
	private BufferedReader br = null;
	
	public void init(Properties p) {
		if(p==null)
		{
			try {
				p = new Properties();
				p.load(WossGather.class.getResourceAsStream("gather.properties"));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		this.p = p;
	}

	@Override
	public void setConfiguration(Configuration conf) {
		this.conf = conf;
	}

	
	@Override
	public Collection<BIDR> gather() throws Exception {
		
		Map<String, BIDR> map = new HashMap<>();
		Collection<BIDR> collection = new ArrayList<>();
		br = new BufferedReader(new InputStreamReader(new FileInputStream("data/radwtmp_test")));
		String line;
		while((line=br.readLine())!=null)
		{
			String[] str = line.split("\\|");
			//判断正确信息，丢弃垃圾数据
			if(str.length>=2)
			{
					if("7".equals(str[2]))
					{
						BIDR bidr = new BIDR(
								str[0],
								str[1],
								new java.sql.Timestamp(Long.parseLong(str[3])),
								null,
								str[4],
								null
								);
						map.put(str[4]+str[1], bidr);
						continue;
					}
					
					if("8".equals(str[2]))
					{
						BIDR bidr = map.get(str[4]+str[1]);
						bidr.setLogout_date(new java.sql.Timestamp(Long.parseLong(str[3])));
						bidr.setTime_deration((int)(Long.parseLong(str[3])-bidr.getLogin_date().getTime()));
						collection.add(bidr);
					}
					
			}
		}
		br.close();
		return collection;
	}

}
