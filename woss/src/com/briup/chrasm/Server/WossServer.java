package com.briup.chrasm.Server;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collection;
import java.util.Properties;

import com.briup.chrasm.Configuration.WossConfiguration;
import com.briup.util.BIDR;
import com.briup.util.Configuration;
import com.briup.woss.ConfigurationAWare;
import com.briup.woss.server.Server;

public class WossServer implements Server, ConfigurationAWare {

	private Properties p;
	private ServerSocket ss;
	private Configuration conf;
	@Override
	public void init(Properties p) {
		if(p==null){
			p=new Properties();
			try {
				p.load(WossServer.class.getResourceAsStream("server.properties"));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		this.p=p;
		try {
			ss = new ServerSocket(Integer.parseInt(p.getProperty("port","10001")));
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void setConfiguration(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public Collection<BIDR> revicer() throws Exception {
		Socket socket = ss.accept();
		ObjectInputStream oos = new ObjectInputStream(socket.getInputStream());
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
		Collection<BIDR> list = null;
		while(true){
			Object obj = oos.readObject();
			if(obj instanceof Collection<?>){
				list = (Collection<BIDR>)obj;
				bw.write("OK");
				bw.newLine();
				bw.flush();
			}else {
				bw.write("error");
				bw.newLine();
				bw.flush();
				continue;
			}
			oos.close();
			bw.close();
			break;
		}
		socket.close();
		return list;
	}

	@Override
	public void shutdown() {
		try {
			ss.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
