package db;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

public class DynamicObj {

  private ArrayList<Map<String, Object>> rows = null;
  private ResultSetMetaData metaData = null;
  private int columnCount = 0;
  private ResultSet resultSet = null;

  public void setupDynamicObj(ResultSet resultSet) throws SQLException {
    this.resultSet = resultSet;
    try {
      this.metaData = this.resultSet.getMetaData();
    } catch (SQLException e) {
      throw e;
    }
    try {
      this.columnCount = this.metaData.getColumnCount();
    } catch (SQLException e) {
      throw e;
    }
    this.rows = new ArrayList<Map<String, Object>>();
  }

  public void setRows(ArrayList<Map<String, Object>> rows) {
    this.rows = rows;
  }

  public ArrayList<Map<String, Object>> getRows() {
    return this.rows;
  }

  public ArrayList<Map<String, Object>> processData(ResultSet rs) throws SQLException {
    this.setupDynamicObj(rs);
    
    try {
      while (this.resultSet.next()) {
        Map<String, Object> columns = new LinkedHashMap<String, Object>();

        for (int i = 1; i <= columnCount; i++) {
          columns.put(this.metaData.getColumnLabel(i), this.resultSet.getObject(i));
        }

        this.rows.add(columns);
      }
    } catch (SQLException e) {
      throw e;
    }
    return this.getRows();
  }
}
