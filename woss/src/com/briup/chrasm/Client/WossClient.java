package com.briup.chrasm.Client;



//客户端模块


//相当于发送端，把数据发送到某个服务端

/*
 * 
 * 
 */


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Collection;
import java.util.Properties;

import com.briup.util.BIDR;
import com.briup.util.Configuration;
import com.briup.woss.ConfigurationAWare;
import com.briup.woss.client.Client;

public class WossClient implements Client, ConfigurationAWare {

	private Properties p;
	private Configuration conf;

	@Override
	public void init(Properties p) {
		if(p==null){
			p=new Properties();
			try {
				p.load(WossClient.class.getResourceAsStream("client.properties"));
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
	public void send(Collection<BIDR> list) throws Exception {
		Socket socket = new Socket(p.getProperty("host","127.0.0.1"), Integer.parseInt(p.getProperty("port","100001")));
		ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
		oos.writeObject(list);
		oos.flush();
		BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
		String str;
		while((str=br.readLine())!=null){
			if("OK".equals(str)){
				break;
			}else if("error".equals(str)){
				oos.writeObject(list);
				oos.flush();
			}
		}
		oos.close();
		br.close();
		socket.close();
	}

}
