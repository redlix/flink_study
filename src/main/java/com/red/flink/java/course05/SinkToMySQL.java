package com.red.flink.java.course05;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * <pre>
 *  Version         Date            Author          Description
 * ------------------------------------------------------------
 *  1.0.0           2019/06/10     red        -
 * </pre>
 *
 * @author red
 * @version 1.0.0 2019-06-10 09:45
 * @since 1.0.0
 */
public class SinkToMySQL extends RichSinkFunction<Student> {
	Connection connection;
	PreparedStatement statement;

	private Connection getConnection() {
		Connection conn = null;

		try {
			Class.forName("com.mysql.cj.jdbc.Driver");
			String url = "jdbc:mysql://112.74.36.53:3306/test";
			conn = DriverManager.getConnection(url, "root", "redli9600,.0@");
		} catch (Exception e) {
			e.printStackTrace();
		}
		return conn;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		connection = getConnection();
		String sql = "insert into student(id,name,age) values (?,?,?)";
		statement = connection.prepareStatement(sql);
	}

	@Override
	public void invoke(Student value, Context context) throws Exception {
		System.out.println("invoke------");
		statement.setInt(1, value.getId());
		statement.setString(2, value.getName());
		statement.setInt(3, value.getAge());

		statement.executeUpdate();
	}

	@Override
	public void close() throws Exception {
		super.close();
		if (statement != null) {
			statement.close();
		}
		if (connection != null) {
			connection.close();
		}
	}
}
