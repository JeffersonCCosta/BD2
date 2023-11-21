package com.oracle.tutorial.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class MyQueries3 {
  
  Connection con;
  JDBCUtilities settings;  
  
  public MyQueries3(Connection connArg, JDBCUtilities settingsArg) {
    this.con = connArg;
    this.settings = settingsArg;
  }

  public static void getMyData(Connection con) throws SQLException {
    Statement stmt = null;
    String query =
      "select distinct d.nome_cliente, sum(d.saldo_deposito), sum(e.valor_emprestimo) " +
      "from deposito as d full outer join emprestimo as e " +
      "on d.nome_cliente = e.nome_cliente " + 
      "where e.valor_emprestimo is not null " + 
      "and d.saldo_deposito is not null "+
      "group by d.nome_cliente;";

    try {
      stmt = con.createStatement();
      ResultSet rs = stmt.executeQuery(query);
      System.out.println("Soma de depositos e emprestimos dos clientes: ");
      while (rs.next()) {
        String Name = rs.getString(1);
        System.out.println("     " + Name);
        String deposito = rs.getString(2);
        System.out.println("              DEP - " + deposito);
        String emprestimo = rs.getString(3);
        System.out.println("              EMP - " + emprestimo);
      }
    } catch (SQLException e) {
      JDBCUtilities.printSQLException(e);
    } finally {
      if (stmt != null) { stmt.close(); }
    }
  }


  public static void main(String[] args) {
    JDBCUtilities myJDBCUtilities;
    Connection myConnection = null;
    if (args[0] == null) {
      System.err.println("Properties file not specified at command line");
      return;
    } else {
      try {
        myJDBCUtilities = new JDBCUtilities(args[0]);
      } catch (Exception e) {
        System.err.println("Problem reading properties file " + args[0]);
        e.printStackTrace();
        return;
      }
    }

    try {
      myConnection = myJDBCUtilities.getConnection();

 	MyQueries3.getMyData(myConnection);

    } catch (SQLException e) {
      JDBCUtilities.printSQLException(e);
    } finally {
      JDBCUtilities.closeConnection(myConnection);
    }

  }
}