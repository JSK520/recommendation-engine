package com.briup.chrasm.DBStore;

//Èë¿âÄ£¿é

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Properties;

import com.briup.chrasm.Configuration.WossConfiguration;
import com.briup.util.BIDR;
import com.briup.util.Configuration;
import com.briup.woss.ConfigurationAWare;
import com.briup.woss.server.DBStore;

public class WossDBStore implements DBStore, ConfigurationAWare {

	private Properties p;
	private Connection conn;
	private Configuration conf;
	@Override
	public void init(Properties p) {
		if(p==null){
			p=new Properties();
			try {
				p.load(WossDBStore.class.getResourceAsStream("jdbc.properties"));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		this.p=p;
		
		try {
			Class.forName("com.mysql.jdbc.Driver");
			this.conn = DriverManager.getConnection(p.getProperty("url","jdbc:mysql://127.0.0.1:3306")+"/"+p.getProperty("database","woss"),p.getProperty("name","root"),p.getProperty("passwd","root"));
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	@Override
	public void setConfiguration(Configuration conf) {
		this.conf = conf;

	}

	@Override
	public void saveToDB(Collection<BIDR> list) throws Exception {
		if(list == null)return;
		String sql = "insert into t_detail values(?,?,?,?,?,?)";
		PreparedStatement ps = conn.prepareStatement(sql);
		for(BIDR i:list){
			ps.setString(1, i.getAAA_login_name());
			ps.setString(2, i.getLogin_ip());
			ps.setTimestamp(3, i.getLogin_date());
			ps.setTimestamp(4, i.getLogout_date());
			ps.setString(5, i.getNAS_ip());
			ps.setInt(6, i.getTime_deration());
			
			ps.execute();
			
		}

	}

}
