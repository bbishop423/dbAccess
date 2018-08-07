
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Scanner;

import db.DbConnection;
import db.DbType;

public class Main {

  public static void main(String[] args) {
    ArrayList<Map<String, Object>> table = null;
    
    ArrayList<String> dbSetupInfo = getInput();
    String ip = dbSetupInfo.get(0);
    String port = dbSetupInfo.get(1);
    String serviceName = dbSetupInfo.get(2);
    String userName = dbSetupInfo.get(3);
    String password = dbSetupInfo.get(4);

    try(DbConnection db = new DbConnection(DbType.ORACLE, ip, port, serviceName, userName, password)) {
      db.run("select to_char(sysdate, 'DD-MON-YY HH24:MI:SS') as \"DATE\" from dual");
      table = db.getDynamicResults();
    } catch (SQLException | ClassNotFoundException e) {
      System.out.println("Unexpected error: " + e.toString());
      e.printStackTrace();
    }

    for (Map<String, Object> row : table) {
      for (String key : row.keySet()) {
        Object value = row.get(key);
        System.out.println("key = " + key);
        System.out.println("value = " + value.toString());
        System.out.println("value datatype = " + value.getClass().toString());
      }
    }
  }

  public static ArrayList<String> getInput() {
    ArrayList<String> dbSetupInfo = new ArrayList<String>();
    String ip = "";
    String port = null;
    String serviceName = "";
    String username = "";
    String password = "";
    Scanner scan = new Scanner(System.in);

    System.out.print("Enter ip address: ");
    ip = scan.next();
    System.out.print("Enter port number: ");
    port = scan.next();
    System.out.print("Enter service name: ");
    serviceName = scan.next();
    System.out.print("Enter username: ");
    username = scan.next();
    System.out.print("Enter password: ");
    password = scan.next();
    scan.close();

    dbSetupInfo.add(ip);
    dbSetupInfo.add(port);
    dbSetupInfo.add(serviceName);
    dbSetupInfo.add(username);
    dbSetupInfo.add(password);

    return dbSetupInfo;
  }
}
