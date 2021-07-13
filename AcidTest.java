package pace;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;

public class AcidTest {

	//by Mudassir Ali and Ileana Kennedy

		public static void main(String args[]) throws SQLException, IOException, ClassNotFoundException{
			
			String url       = "jdbc:postgresql://localhost:5432/postgres";
			String user      = "postgres";
			String password  = "1234";
			 
			// Load the MySQL driver
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
						
						connectionStatement.executeUpdate("CREATE TABLE depot (primary key(depid), address varchar (15), quanity integer");
						
						// Either the 2 following inserts are executed, or none of them are. This is atomicity.

	
						//At first we try to add two rows with the same depid but the ID must be unique, so the changes don't go though. This is consistency 
						
						connectionStatement.executeUpdate("INSERT INTO depot(\"depid\", \"address\", \"quantity\") VALUES (1,'MONTANA',10);");
						connectionStatement.executeUpdate("INSERT INTO depot(\"depid\", \"address\", \"quantity\") VALUES (1,'IDAHO',20);");
						
						connectionStatement.executeUpdate("INSERT INTO stock (\"prodid\", \"depid\", \"quantity\") VALUES (1,1,200);");
						
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








