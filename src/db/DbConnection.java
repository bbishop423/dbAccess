package db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Map;

public class DbConnection implements AutoCloseable {
  private Connection con = null;
  private Statement stmt = null;
  private ResultSet rs = null;
  private int rowsUpdated = 0;
  private String driverName = null;
  private String connectionString = null;
  private DynamicObj dyn = new DynamicObj();

  public DbConnection(DbType dbType, String ip, String port, String dbName, String userName, String password) 
      throws ClassNotFoundException, SQLException {
    this.setDriverNameConString(dbType, ip, port, dbName);
    try {
      Class.forName(this.driverName);
      this.con = DriverManager.getConnection(this.connectionString, userName, password);
      this.con.setAutoCommit(false);
      this.stmt = con.createStatement();
    } catch (ClassNotFoundException e) {
      throw e;
    } catch (SQLException e) {
      throw e;
    }
  }

  private void setDriverNameConString(DbType dbType, String ip, String port, String dbName) {
    this.driverName = dbType.getDriverName();
    this.connectionString = dbType.getConnectionString(ip, port, dbName);
  }
  
  public int getRowsUpdated() {
    return this.rowsUpdated;
  }

  public void setRowsUpdated(int rowsUpdated) {
    this.rowsUpdated = rowsUpdated;
  }

  public Connection getCon() {
    return this.con;
  }

  public void setCon(Connection con) {
    this.con = con;
  }

  public void close() throws SQLException {
    this.manualClose();
  }

  public void manualClose() throws SQLException {
    try {
      if (this.rs != null) {
        this.rs.close();
      }
    } catch (SQLException e) {
      throw e;
    }
    
    try {
      if (this.stmt != null) {
        this.stmt.close();
      }
    } catch (SQLException e) {
      throw e;
    }
    
    try {
      if (this.con != null) {
        this.con.close();
      }
    } catch (SQLException e) {
      throw e;
    }
  }

  public int runUpdate(String sql) throws SQLException {
    try {
      this.setRowsUpdated(this.stmt.executeUpdate(sql));
    } catch (SQLException e) {
      if(this.con != null) {
        try {
          this.con.rollback();
        } catch (SQLException e1) {
          throw e1;
        }
      }
      throw e;
    }
    return this.getRowsUpdated();
  }

  public void commit() throws SQLException {
    try {
      this.getCon().commit();
    } catch (SQLException e) {
      try {
        this.getCon().rollback();
      } catch (SQLException e1) {
        throw e1;
      }
      throw e;
    }
  }

  public void rollback() throws SQLException {
    try {
      this.getCon().rollback();
    } catch (SQLException e) {
      throw e;
    }
  }
  
  public ArrayList<ArrayList<String>> getResults() throws SQLException {
    ArrayList<ArrayList<String>> resultList = new ArrayList<ArrayList<String>>();

    try {
      ResultSetMetaData metaData = this.rs.getMetaData();
      int columnCount = metaData.getColumnCount();

      while (this.rs.next()) {
        ArrayList<String> row = new ArrayList<String>();
        for (int i = 1; i <= columnCount; i++) {
          row.add(this.rs.getString(i));
        }
        resultList.add(row);
      }
    } catch (SQLException e) {
      throw e;
    }
    return resultList;
  }

  public void run(String sql) throws SQLException {
    try {
      this.rs = this.stmt.executeQuery(sql);
    } catch (SQLException e) {
      throw e;
    }
  }
  
  public ArrayList<ArrayList<String>> getStringResults() throws SQLException {
    return this.getResults();
  }
  
  public ArrayList<Map<String, Object>> getDynamicResults() throws SQLException {
    ArrayList<Map<String, Object>> results = null;
    try {
      results = this.dyn.processData(this.rs);
    } catch (SQLException e) {
      throw e;
    }
    return results;
  }
}
