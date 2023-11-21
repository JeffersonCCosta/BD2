package com.oracle.tutorial.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.Scanner;
import java.sql.DatabaseMetaData;
import java.sql.Date;

public class MyQueries {
  
  Connection con;
  JDBCUtilities settings;  
  
  public MyQueries(Connection connArg, JDBCUtilities settingsArg) {
    this.con = connArg;
    this.settings = settingsArg;
  }

  public static void getMyData(Connection con) throws SQLException {
    Statement stmt = null;
    String query =
      "SELECT SUPPLIERS.SUP_NAME, COUNT(COFFEES.COF_NAME) AS QTD FROM COFFEES INNER JOIN SUPPLIERS ON SUPPLIERS.SUP_ID = COFFEES.SUP_ID GROUP BY SUPPLIERS.SUP_NAME";

    try {
      stmt = con.createStatement();
      ResultSet rs = stmt.executeQuery(query);
      System.out.println("Quantidade de cafe fornecidos por cada fornecedor: ");
      while (rs.next()) {
        String coffeeName = rs.getString(1);
        System.out.println("    " + rs.getString("SUP_NAME") + "     " + rs.getString("QTD"));
      }
    } catch (SQLException e) {
      JDBCUtilities.printSQLException(e);
    } finally {
      if (stmt != null) { stmt.close(); }
    }
  }

   public static void populateTable(Connection con) throws SQLException, IOException {
    BufferedReader inputStream = null;
    Scanner scanned_line = null;
    String line;
    String[] value;
    value = new String[7];
    int countv;
    Statement stmt = null;
    String create = "";
    con.setAutoCommit(false);
    try {
      stmt = con.createStatement();

      inputStream = new BufferedReader(new FileReader("/home/vboxuser/Downloads/debito-populate-table.txt"));
      stmt.executeUpdate("truncate table debito;");
      
      while ((line = inputStream.readLine()) != null) {
        countv=0;
        scanned_line = new Scanner(line);
        scanned_line.useDelimiter("\t");
        while (scanned_line.hasNext()) {
          value[countv++]=scanned_line.next();
        } //while
        if (scanned_line != null) { scanned_line.close(); }
        
        stmt.addBatch("insert into debito (numero_debito, valor_debito, motivo_debito, data_debito, numero_conta, nome_agencia, nome_cliente) " +"values (" + value[0] +", "+ value[1] +", "+ value[2] +", '"+ value[3] +"', "+ value[4] +", '"+ value[5] +"', '"+ value[6] + "');");
      } //while
      
      int[] updateCounts = stmt.executeBatch();
      con.commit();
      
    } catch (SQLException e) {
      JDBCUtilities.printSQLException(e);
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      con.setAutoCommit(true);
      if (stmt != null) { stmt.close(); }
    }
}
    
    public static void getMyData3(Connection con) throws SQLException {     
    	Statement stmt = null;     
    	String query = "SELECT cliente.nome_cliente, sum(emprestimo.valor_emprestimo), sum(deposito.saldo_deposito) FROM cliente inner join deposito on cliente.nome_cliente = deposito.nome_cliente inner join emprestimo on emprestimo.nome_cliente = cliente.nome_cliente inner join CONTA on conta.nome_cliente = cliente.nome_cliente group by cliente.nome_cliente, conta.nome_agencia,conta.numero_conta";    
    	try {       
    		stmt = con.createStatement();       
    		ResultSet rs = stmt.executeQuery(query);       
    		System.out.println("Consulta exercício 2: ");       
    		while (rs.next()) { 
    			String nome = rs.getString(1);         
    			String emprestimo = rs.getString(2);   
    			String deposito = rs.getString(3);      
    			System.out.println(nome +", " + emprestimo + ", "+ deposito);}     
    	} catch (SQLException e) { 
    		JDBCUtilities.printSQLException(e);     
    	} finally {       
    		if (stmt != null) { 
    			stmt.close(); 
    			}   
    		 }  
    	 }
    	 
   public static void cursorHoldabilitySupport(Connection conn)     throws SQLException {     
	   DatabaseMetaData dbMetaData = conn.getMetaData();     
	   System.out.println("ResultSet.HOLD_CURSORS_OVER_COMMIT = " +  ResultSet.HOLD_CURSORS_OVER_COMMIT); 
	   System.out.println("ResultSet.CLOSE_CURSORS_AT_COMMIT = " +   ResultSet.CLOSE_CURSORS_AT_COMMIT); 
	   System.out.println("Default cursor holdability: " +         dbMetaData.getResultSetHoldability());
	   System.out.println("Supports HOLD_CURSORS_OVER_COMMIT? " +
	   dbMetaData.supportsResultSetHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT));     
	   System.out.println("Supports CLOSE_CURSORS_AT_COMMIT? " +       
	   dbMetaData.supportsResultSetHoldability(             ResultSet.CLOSE_CURSORS_AT_COMMIT)); 
   }
   public static void supportResultSetConcurrency(Connection conn) throws SQLException,IOException {     
	  DatabaseMetaData dbMetaData = conn.getMetaData();  
	  
	  System.out.println("Support TYPE_FORWARD_ONLY CONCUR_READ_ONLY = " + 		dbMetaData.supportsResultSetConcurrency(ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_READ_ONLY));
	  System.out.println("Support TYPE_FORWARD_ONLY CONCUR_UPDATABLE = " + dbMetaData.supportsResultSetConcurrency(ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_UPDATABLE));
	  
	  System.out.println("Support TYPE_SCROLL_INSENSITIVE CONCUR_READ_ONLY = " + dbMetaData.supportsResultSetConcurrency(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY));
	  System.out.println("Support TYPE_SCROLL_INSENSITIVE CONCUR_UPDATABLE = " + dbMetaData.supportsResultSetConcurrency(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_UPDATABLE));
	  
	  System.out.println("Support TYPE_SCROLL_SENSITIVE CONCUR_READ_ONLY = " + dbMetaData.supportsResultSetConcurrency(ResultSet.TYPE_SCROLL_SENSITIVE,ResultSet.CONCUR_READ_ONLY));
	  System.out.println("Support TYPE_SCROLL_SENSITIVE CONCUR_UPDATABLE = " + dbMetaData.supportsResultSetConcurrency(ResultSet.TYPE_SCROLL_SENSITIVE,ResultSet.CONCUR_UPDATABLE));
  }
  
  public static void modifyPrices(Connection con) throws SQLException,IOException { 
  	System.out.println("Digite o multiplicador como um numero real (Ex.: 5% = 1,05):"); 
  	Scanner in = new Scanner(System.in); 
  	double percentage = in.nextDouble();
  	
	Statement stmt = null;     
	try {                 
		stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);         
		ResultSet uprs = stmt.executeQuery( "SELECT * FROM deposito");         
		while (uprs.next()) {             
			float f = uprs.getFloat("saldo_deposito");         
			uprs.updateDouble("saldo_deposito", f * percentage);             
			uprs.updateRow();         
		} 
		System.out.println("Depositos Atualizados");    
	} catch (SQLException e ) {         
	  	JDBCTutorialUtilities.printSQLException(e);     
	} finally {         
		if (stmt != null) { 
		  stmt.close(); 
	  	}	     
	} 
  }  
  public static void insertRow(Connection con,  Double valor_debito, int motivo, String data_debito, int Conta, String agencia, String Cliente )  throws SQLException,IOException {     
	  Statement stmt = null;     
	  try {         
		  stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);         
		  ResultSet uprs = stmt.executeQuery("SELECT * FROM debito");     
		  uprs.last();
		  int numero_debito = uprs.getInt("numero_debito");
		  uprs.moveToInsertRow(); //posiciona no ponto de inserção da tabela        
		  uprs.updateInt("numero_debito", numero_debito + 1);    
		  uprs.updateDouble("valor_debito", valor_debito);
		  uprs.updateInt("motivo_debito", motivo);     
		  uprs.updateDate("data_debito", Date.valueOf(data_debito));
		  uprs.updateInt("numero_conta", Conta);
		  uprs.updateString("nome_agencia", agencia);
		  uprs.updateString("nome_cliente", Cliente);
		  uprs.insertRow(); //insere a linha na tabela        
		  uprs.beforeFirst(); //posiciona-se novamente na posição anterior ao primeiro registro  
		  
		System.out.println("Debito feito com sucesso: " + Cliente);
		
	  } catch (SQLException e ) {  
	         
	  	JDBCTutorialUtilities.printSQLException(e);     
	  } finally {   
	        
		  if (stmt != null) { 
		  	stmt.close(); 
		  }     
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
      //populateTable(myConnection);
      //MyQueries.getMyData3(myConnection);
      //cursorHoldabilitySupport(myConnection);
      //supportResultSetConcurrency(myConnection);
      //modifyPrices(myConnection);
 	  //MyQueries.getMyData(myConnection);
      insertRow(myConnection,150.0,1,"2014-01-23",46248,"UFU","Carla Soares Sousa");
      insertRow(myConnection,200.0,2,"2014-01-23",26892,"Glória","Carolina Soares Souza");
      insertRow(myConnection,500.0,3,"2014-01-23",70044,"Cidade Jardim","Eurides Alves da Silva");

    } catch (SQLException e) {
      JDBCUtilities.printSQLException(e);
    } 
    catch (IOException e) {
     e.printStackTrace();
    }
    finally {
      JDBCUtilities.closeConnection(myConnection);
    }

  }
  
  
}


