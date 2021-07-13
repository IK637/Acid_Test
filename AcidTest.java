package pace;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;

public class AcidTest2 {

	//by Mudassir Ali and Ileana Kennedy

		public static void main(String args[]) throws SQLException, IOException, ClassNotFoundException{
			
			String url       = "jdbc:postgresql://localhost:5432/postgres";
			String user      = "postgres";
			String password  = "1234";
			 
			// Load the postgreSQL driver
					Class.forName("org.postgresql.Driver");

					System.out.println("Connecting Driver");
					// Connect to the database
					Connection conn = DriverManager.getConnection(url,user,password);
					System.out.println("Connected");
					
					// For Atomicity
					conn.setAutoCommit(false);
					
					// For Isolation 
					conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE); 
					
					Statement connectionStatement = null;
					try {
						// create statement object
						connectionStatement = conn.createStatement();
						System.out.println("Statement Created");
						
						// Attempts to create a table called depot, but one already exists so nothing gets committed
						
						// Building the initial tables Depot and Product
						
						//connectionStatement.executeUpdate("CREATE TABLE depot (primary key(depid), address varchar (15), volume integer");
						//connectionStatement.executeUpdate("CREATE TABLE stock (primary key(prodid,depid), address varchar (15), quanity integer");
						
						connectionStatement.executeUpdate("INSERT INTO depot(\"depid\", \"address\", \"volume\") VALUES (d1,'New York',9000);");
						connectionStatement.executeUpdate("INSERT INTO depot(\"depid\", \"address\", \"volume\") VALUES (d2,'Syracuse',6000);");
						connectionStatement.executeUpdate("INSERT INTO depot(\"depid\", \"address\", \"volume\") VALUES (d4,'New York',2000);");
						
						connectionStatement.executeUpdate("INSERT INTO stock(\"prodid\", \"depid\", \"quantity\") VALUES (p1,d1,1000);");
						connectionStatement.executeUpdate("INSERT INTO stock(\"prodid\", \"depid\", \"quantity\") VALUES (p1,d2,-200);");
						connectionStatement.executeUpdate("INSERT INTO stock(\"prodid\", \"depid\", \"quantity\") VALUES (p1,d4,1200);");
						connectionStatement.executeUpdate("INSERT INTO stock(\"prodid\", \"depid\", \"quantity\") VALUES (p3,d1,3000);");
						connectionStatement.executeUpdate("INSERT INTO stock(\"prodid\", \"depid\", \"quantity\") VALUES (p3,d4,2000);");
						connectionStatement.executeUpdate("INSERT INTO stock(\"prodid\", \"depid\", \"quantity\") VALUES (p2,d4,1500);");
						connectionStatement.executeUpdate("INSERT INTO stock(\"prodid\", \"depid\", \"quantity\") VALUES (p2,d1,-400);");
						connectionStatement.executeUpdate("INSERT INTO stock(\"prodid\", \"depid\", \"quantity\") VALUES (p2,d2,2000);");
						
						// Either the 2 following inserts are executed, or none of them are. This is atomicity.
						connectionStatement.executeUpdate("INSERT INTO stock (\"prodid\", \"depid\", \"quantity\") VALUES (p1,d100,100);");
						connectionStatement.executeUpdate("INSERT INTO depot (\"depid\", \"address\", \"volume\") VALUES (d100,'Chicago',100);");
	
						//we try to add two rows with the same depid but the ID must be unique, so the changes don't go though. This is consistency 

						connectionStatement.executeUpdate("INSERT INTO depot (\"depid\", \"address\", \"volume\") VALUES (d20,Chicago,100);");
						connectionStatement.executeUpdate("INSERT INTO depot (\"depid\", \"address\", \"volume\") VALUES (d20,New York,200);");
						

						
						System.out.println("values added");
					} catch (SQLException e) {
						System.out.println("catch Exception");
						
						// For Atomicity
						conn.rollback();
						connectionStatement.close();
						conn.close();
						return;
					} 
					
					//for Durability
					conn.commit();
					connectionStatement.close();
					conn.close();	
			
			
		}
	
}
