package WossServer;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collection;
import java.util.Properties;

import com.briup.util.BIDR;
import com.briup.util.Configuration;
import com.briup.woss.ConfigurationAWare;
import com.briup.woss.server.Server;

import WossClient.WossClient;

public class WossServer implements Server, ConfigurationAWare {
	
	private Properties p;
	private ServerSocket server;
	Configuration conf;
	
	public void init(Properties p) {
		if(p==null)
		{
			try {
				p = new Properties();
				p.load(WossServer.class.getResourceAsStream("conf.properties"));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		try {
			server = new ServerSocket(Integer.parseInt(p.getProperty("port","11111")));
			this.p = p;
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
		
		Socket socket = server.accept();
		ObjectInputStream oos = new ObjectInputStream(socket.getInputStream());
		PrintWriter pw = new PrintWriter(socket.getOutputStream());
		Collection<BIDR> list = null;
		while(true)
		{
			Object obj = oos.readObject();
			if(obj instanceof Collection<?>)
			{
				list = (Collection<BIDR>)obj;
				pw.println("ok");
				pw.flush();
			}
			else{
				pw.println("error");
				pw.flush();
			}
			
			oos.close();
			pw.close();
			break;
		}
		socket.close();
		return list;
	}

	public void shutdown() {
		try {
			server .close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
