package WossClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.nio.Buffer;
import java.util.Collection;
import java.util.Properties;

import com.briup.util.BIDR;
import com.briup.util.Configuration;
import com.briup.woss.ConfigurationAWare;
import com.briup.woss.client.Client;

import WossGather.WossGather;

public class WossClient implements Client, ConfigurationAWare {

	private Properties p;
	private Configuration conf;
	
	public void init(Properties p) {
		if(p==null)
		{
			try {
				p = new Properties();
				p.load(WossClient.class.getResourceAsStream("client.properties"));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		this.p = p;
	}

	public void setConfiguration(Configuration conf) {
		this.conf = conf;
	}

	public void send(Collection<BIDR> list) throws Exception {
		
		Socket socket = new Socket(p.getProperty("host","127.0.0.1"),Integer.parseInt(p.getProperty("port","11111")));
		ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
		oos.writeObject(list);
		oos.flush();
		BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
		String str;
		while((str=br.readLine())!=null)
		{
			if("ok".equals(str))
			{
				break;
			}
			if("error".equals(str)){
				oos.writeObject(list);
				oos.flush();
			}
		}
		
		
		br.close();
		oos.close();
		socket.close();
	}

}
