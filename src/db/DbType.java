package db;

public enum DbType {
  
  ORACLE("oracle.jdbc.driver.OracleDriver", "jdbc:oracle:thin:@", ":", "/"),
  POSTGRESQL("org.postgresql.Driver", "jdbc:postgresql://", ":", "/"),
  MYSQL("com.mysql.jdbc.Driver", "jdbc:mysql://", ":", "/"),
  DB2("COM.ibm.db2.jdbc.net.DB2Driver", "jdbc:db2:", ":", "/"),
  SYBASE("com.sybase.jdbc.SybDriver", "jdbc:sybase:Tds:", ":", "/"),
  MSSQLSERVER("com.microsoft.jdbc.sqlserver.SQLServerDriver", "jdbc:microsoft:sqlserver://", ":", ";DatabaseName=");

  private final String driverName;
  private final String prefix;
  private final String separator;
  private final String postfix;

  private DbType(String driverName, String prefix, String separator, String postfix) {
    this.driverName = driverName;
    this.prefix = prefix;
    this.separator = separator;
    this.postfix = postfix;
  }

  public String getDriverName() {
    return this.driverName;
  }

  public String getConnectionString(String ip, String port, String dbName) {
    return this.prefix + ip + this.separator + port + this.postfix + dbName;
  }
}
