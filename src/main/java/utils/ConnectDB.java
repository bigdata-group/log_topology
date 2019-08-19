package utils;

import java.sql.Connection;
import java.sql.DriverManager;
//import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class ConnectDB {
	String user = "root";
	String password = "root";
	String url = "jdbc:mysql://localhost:3306/liumeng";
	String driver = "com.mysql.jdbc.Driver";
	//String driver = "org.gjt.mm.mysql.Driver";
	//String tableName = "kdt_pageid";
	String sqlstr;
	Connection conn = null;
	Statement stmt = null;
	ResultSet rs = null;
	
	
	public Connection ConnectMysql(){
		
		try{
			Class.forName(driver);
			conn = (Connection) DriverManager.getConnection(url, user, password);
			if (!conn.isClosed()) {
				System.out.println("Succeeded connecting to the Database!");
			} else {
				System.out.println("Failed connecting to the Database!");
			}
		}catch(Exception ex){
			ex.printStackTrace();
		}
		
		return conn;
	}
	
    public void CutConnection(Connection conn) throws SQLException{
        try{
        	if(rs!=null);
        	if(conn!=null);
        }catch(Exception e){
	        e.printStackTrace();
        }finally{
        	rs.close();
        	conn.close();
        }
    }

}

