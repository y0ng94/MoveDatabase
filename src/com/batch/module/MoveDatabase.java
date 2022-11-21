package com.batch.module;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import yn.util.CommonUtil;
import yn.util.Config;
import yn.util.LogUtil;

public class MoveDatabase {
	private static Logger logger = LoggerFactory.getLogger(MoveDatabase.class);
	
	public void run() {
		// 기본 파라미터가 적힌 파일을 찾기, 없으면 풀 경로 검색
		long time = System.currentTimeMillis();
		LogUtil.info(logger, "Find parameter list file for query binding");

		Path paramFilePath = Paths.get(Config.getConfig("FROM.DB.PARAM"));
		if (Files.notExists(paramFilePath)) { paramFilePath = Paths.get(Config.getConfig("BASEDIR"), Config.getConfig("FROM.DB.PARAM")); }
		if (Files.notExists(paramFilePath)) {
			LogUtil.error(logger, "Parameter file does not exist...");
			return;
		}

		LogUtil.info(logger, "Completed finding parameter list file for query binding ( {0}s )", CommonUtil.getTimeElapsed(time));

		// 파라미터 파일 읽어서 리스트에 담기
		time = System.currentTimeMillis();
		LogUtil.info(logger, "Reading parameter list file for query binding");

		List<String[]> paramList = new ArrayList<>();		
		try {
			Files.readAllLines(paramFilePath).stream().forEach(t -> paramList.add(t.split("\t")));
		} catch (IOException e) {
			e.printStackTrace();
			LogUtil.error(logger,"Error occurred during reading parameter list file ({0})", e.getMessage());
			return;
		}

		LogUtil.info(logger, "Completed reading parameter list file for query binding ( {0}s )", CommonUtil.getTimeElapsed(time));

		// 메모리에 클래스 올리기
		time = System.currentTimeMillis();
		try {
			Class.forName(Config.getConfig("FROM.DB.CLASS"));
			Class.forName(Config.getConfig("TO.DB.CLASS"));
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			LogUtil.error(logger, "Error occurred during load database driver ({0})", e.getMessage());
			return;
		}

		LogUtil.info(logger, "Completed loading database driver ( {0}s )", CommonUtil.getTimeElapsed(time));

		// 파라미터의 갯수만큼 반복
		int totalCount = 0;
		for (String[] param : paramList) {
			LogUtil.info(logger, "Checking target parameter ( {0} )", Arrays.asList(param).stream().collect(Collectors.joining(", ")));

			// 커넥션 객체 만들기
			time = System.currentTimeMillis();
			Connection fromCon = null;
			Connection toCon = null;
			try {
				LogUtil.info(logger, "Creating connection of database(from) ( {0} / {1} / {2} )", Config.getConfig("FROM.DB.URL"), Config.getConfig("FROM.DB.USER"), Config.getConfig("FROM.DB.PASSWORD"));
				fromCon = getConnection(Config.getConfig("FROM.DB.URL"), Config.getConfig("FROM.DB.USER"), Config.getConfig("FROM.DB.PASSWORD"));
				LogUtil.info(logger, "Creating connection of database(to) ( {0} / {1} / {2} )", Config.getConfig("TO.DB.URL"), Config.getConfig("TO.DB.USER"), Config.getConfig("TO.DB.PASSWORD"));
				toCon = getConnection(Config.getConfig("TO.DB.URL"), Config.getConfig("TO.DB.USER"), Config.getConfig("TO.DB.PASSWORD"));
			} catch (NullPointerException | SQLException e) {
				e.printStackTrace();
				LogUtil.error(logger, "Error occurred during create connection of database ({0})", e.getMessage());
				return;
			}

			LogUtil.info(logger, "Completed creating connection of database ( {0}s )", CommonUtil.getTimeElapsed(time));

			// AS-IS 데이터 조회해오기
			time = System.currentTimeMillis();
			List<String[]> selectList = new ArrayList<>();
			try {
				LogUtil.info(logger, "Selecting data of database(from)");
				selectList = select(fromCon, Config.getConfig("FROM.DB.QUERY"), param);
			} catch (NullPointerException | SQLException e) {
				e.printStackTrace();
				LogUtil.error(logger, "Error occurred during select of database(from) ({0})", e.getMessage());
				return;
			} finally {
				if (fromCon != null){
					try {
						fromCon.close();
						fromCon = null;
					} catch (SQLException e) {
						e.printStackTrace();
						LogUtil.error(logger, "Error occurred during close of database(from) connection ({0})", e.getMessage());
						return;
					}
				}
			}

			LogUtil.info(logger, "Completed {0} selection data ( {1}s )", selectList.size(), CommonUtil.getTimeElapsed(time));

			// TO-BE 데이터 삽입하기
			int count = 0;
			try {
				if (selectList.size() != 0) {
					LogUtil.info(logger, "Inserting data of database(to)");
					count = insert(toCon, Config.getConfig("TO.DB.QUERY"), selectList);
				}
			} catch (NullPointerException | SQLException e) {
				e.printStackTrace();
				LogUtil.error(logger, "Error occurred during insert of database(to) ({0})", e.getMessage());
				return;
			} finally {
				if (toCon != null){
					try {
						toCon.close();
						toCon = null;
					} catch (SQLException e) {
						e.printStackTrace();
						LogUtil.error(logger, "Error occurred during close of database(to) connection ({0})", e.getMessage());
						return;
					}
				}
			}

			totalCount += count;

			LogUtil.info(logger, "Completed {0} insertion data ( {1}s )", count, CommonUtil.getTimeElapsed(time));
		}
		
		LogUtil.info(logger, "Completed all {0} processing", totalCount);
	}

	/**
	 * 커넥션 객체 만들기
	 * @param url
	 * @param user
	 * @param password
	 * @return Connection 객체
	 * @throws SQLException
	 */
	private Connection getConnection(String url, String user, String password) throws SQLException {
		Connection con = DriverManager.getConnection(url, user, password);
		con.setAutoCommit(false);
		return con;
	}

	/**
	 * 조회 하기
	 * @param con
	 * @param query
	 * @param param
	 * @return 조회 결과 리스트
	 * @throws SQLException
	 */
	private List<String[]> select(Connection con, String query, String[] param) throws SQLException {
		List<String[]> selectList = new ArrayList<>();

		try (PreparedStatement statement = con.prepareStatement(query)) {
			statement.setQueryTimeout(Config.getIntConfig("DB.TIMEOUT"));
			ResultSetMetaData metadata = statement.getMetaData();

			for (int i=0; i<param.length; i++)
				statement.setString(i+1, param[i]);

			try (ResultSet resultSet = statement.executeQuery()) {
				while (resultSet.next()) {
					String[] selectArray = new String[metadata.getColumnCount()];
					
					for (int i=1; i<=metadata.getColumnCount(); i++)
						selectArray[i-1] = resultSet.getString(metadata.getColumnName(i));

					selectList.add(selectArray);
				}
			} catch (SQLException e) {
				throw e;
			}
		} catch (SQLException e) {
			throw e;
		}

		return selectList;
	}

	/**
	 * 삽입 하기
	 * @param con
	 * @param query
	 * @param paramList
	 * @return 데이터 삽입 후 결과
	 * @throws SQLException
	 */
	private int insert(Connection con, String query, List<String[]> paramList) throws SQLException {
		int count = 0;
		PreparedStatement statement = null;

		try {
			statement = con.prepareStatement(query);
			statement.setQueryTimeout(Config.getIntConfig("DB.TIMEOUT"));

			for (int i=0; i<paramList.size(); i++) {
				String[] param = paramList.get(i);

				for (int j=0; j<param.length; j++)
					statement.setString(j+1, param[j]);

				statement.addBatch();
				statement.clearParameters();

				if ((i % 50) == 0) {
					int[] ret = statement.executeBatch();
					statement.clearBatch();
					con.commit();
					
					for (int j=0; j<ret.length; j++) {
						if (ret[j] == Statement.SUCCESS_NO_INFO)
							count = count + 1;
						else if (ret[j] == Statement.EXECUTE_FAILED)
							count = count + 0;
						else
							count = count + ret[j];
					}
				}
			}

			int[] ret = statement.executeBatch();
			statement.clearBatch();
			con.commit();
			
			for (int j=0; j<ret.length; j++) {
				if (ret[j] == Statement.SUCCESS_NO_INFO)
					count = count + 1;
				else if (ret[j] == Statement.EXECUTE_FAILED)
					count = count + 0;
				else
					count = count + ret[j];
			}
		} catch (SQLException e) {
			con.rollback();

			throw e;
		} finally {
			if (statement != null) {
				statement.close();
				statement = null;
			}
		}

		return count;
	}
}
