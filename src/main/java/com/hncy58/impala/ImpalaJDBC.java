package com.hncy58.impala;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.cloudera.impala.jdbc41.DataSource;

public class ImpalaJDBC {

	private static final String DRIVER_CLASS = "com.cloudera.impala.jdbc41.DataSource";
	private static final String CONNECTION_URL = "jdbc:impala://node01:21050";
	private static DataSource ds;

	public static void main(String[] args) throws Exception {
		String sql = "select now()";
		System.out.println(queryForList(CONNECTION_URL, sql));
	}

	private static Connection connectViaDS(String url) throws Exception {
		Connection connection = null;
		Class.forName(DRIVER_CLASS);
		ds = new DataSource();
		ds.setURL(url);
		connection = ds.getConnection();
		return connection;
	}

	public static List<Map<String, Object>> queryForList(String url, String sql) {
		Connection con = null;
		List<Map<String, Object>> ret = new ArrayList<>();
		try {
			con = connectViaDS(url);
			PreparedStatement ps = con.prepareStatement(sql);
			ResultSet rs = ps.executeQuery();
			ResultSetMetaData rsmd = ps.getMetaData();

			while (rs.next()) {
				Map<String, Object> map = new HashMap<>();
				for (int i = 0; i < rsmd.getColumnCount(); i++) {
					map.put(rsmd.getColumnName(i + 1), rs.getObject(i + 1));
				}
				ret.add(map);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (con != null) {
				try {
					con.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		return ret;
	}
}
