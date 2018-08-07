
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

  public DbConnection(DbType dbType, String ip, String port, String dbName, String userName, String password) {
    this.setDriverNameConString(dbType, ip, port, dbName);
    try {
      Class.forName(this.driverName);
      this.con = DriverManager.getConnection(this.connectionString, userName, password);
      this.con.setAutoCommit(false);
      this.stmt = con.createStatement();
    } catch (ClassNotFoundException e) {
      System.out.println("Unexpected Exception: " + e.toString());
    } catch (SQLException e) {
      System.out.println("Unexpected Exception: " + e.toString());
    } catch (Exception e) {
      System.out.println("Unexpected Exception: " + e.toString());
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

  public void close() {
    this.manualClose();
  }

  public void manualClose() {
    try {
      if (this.rs != null) {
        this.rs.close();
      }
    } catch (SQLException e) {
      System.out.println("Unexpected Exception: " + e.toString());
    } catch (Exception e) {
      System.out.println("Unexpected Exception: " + e.toString());
    }
    try {
      if (this.stmt != null) {
        this.stmt.close();
      }
    } catch (SQLException e) {
      System.out.println("Unexpected Exception: " + e.toString());
    } catch (Exception e) {
      System.out.println("Unexpected Exception: " + e.toString());
    }
    try {
      if (this.con != null) {
        this.con.close();
      }
    } catch (SQLException e) {
      System.out.println("Unexpected Exception: " + e.toString());
    } catch (Exception e) {
      System.out.println("Unexpected Exception: " + e.toString());
    }
  }

  public int runUpdate(String sql) {
    try {
      this.setRowsUpdated(this.stmt.executeUpdate(sql));
    } catch (SQLException e) {
      if(this.con != null) {
        try {
          this.con.rollback();
        } catch (SQLException e1) {
          System.out.println("Unexpected Exception: " + e1.toString());
          e1.printStackTrace();
        }
      }
      System.out.println("Unexpected Exception: " + e.toString());
      e.printStackTrace();
    }
    return this.getRowsUpdated();
  }

  public void commit() {
    try {
      this.getCon().commit();
    } catch (SQLException e) {
      try {
        this.getCon().rollback();
      } catch (SQLException e1) {
        System.out.println("Unexpected Exception: " + e1.toString());
        e.printStackTrace();
      }
      System.out.println("Unexpected Exception: " + e.toString());
      e.printStackTrace();
    }
  }

  public void rollback() {
    try {
      this.getCon().rollback();
    } catch (SQLException e) {
      System.out.println("Unexpected Exception: " + e.toString());
      e.printStackTrace();
    }
  }
  
  public ArrayList<ArrayList<String>> getResults() {
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
      System.out.println("Error: " + e.toString());
    }
    return resultList;
  }

  public void run(String sql) {
    try {
      this.rs = this.stmt.executeQuery(sql);
    } catch (SQLException e) {
      System.out.println("Unexpected Exception: " + e.toString());
      e.printStackTrace();
    } catch (Exception e) {
      System.out.println("Unexpected Exception: " + e.toString());
      e.printStackTrace();
    }
  }
  
  public ArrayList<ArrayList<String>> getStringResults() {
    return this.getResults();
  }
  
  public ArrayList<Map<String, Object>> getDynamicResults() {
    return this.dyn.processData(this.rs);
  }
}
