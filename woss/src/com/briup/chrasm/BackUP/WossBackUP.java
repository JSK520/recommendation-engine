package com.briup.chrasm.BackUP;


//±¸·ÝÄ£¿é


import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Properties;

import com.briup.chrasm.Server.WossServer;
import com.briup.util.BackUP;
import com.briup.util.Configuration;
import com.briup.woss.ConfigurationAWare;

public class WossBackUP implements BackUP, ConfigurationAWare {

	private Properties p;
	private Configuration conf;
	
	@Override
	public void init(Properties p) {
		if(p==null){
			p=new Properties();
			try {
				p.load(WossBackUP.class.getResourceAsStream("BackUP.properties"));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		this.p=p;
	}

	@Override
	public void setConfiguration(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public Object load(String key, boolean flag) throws Exception {
		File file = new File(key);
		ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file));
		Object obj = ois.readObject();
		ois.close();
		if(flag)file.delete();
		return obj;
	}

	@Override
	public void store(String key, Object data, boolean flag) throws Exception {
		ObjectOutputStream oos= new ObjectOutputStream(new FileOutputStream(key,flag));
		oos.writeObject(data);
		oos.flush();
		oos.close();
	}
	
}
