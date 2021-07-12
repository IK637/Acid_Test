import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
public class AcidTest {

	public static void main(String args[]) throws SQLException, IOException, ClassNotFoundException{
		
		String url       = "jdbc:mysql://localhost:3306/acidtest?autoReconnect=true&useSSL=false";
		String user      = "root";
		String password  = "123456";
		 
		// Load the MySQL driver
				Class.forName("com.mysql.jdbc.Driver");

				// Connect to the database
				Connection conn = DriverManager
						.getConnection(url,user,password);
				System.out.println("Connected");
				
				// For atomicity
				conn.setAutoCommit(false);
				
				// For isolation 
				conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE); 
				
				Statement connectionStatement = null;
				try {
					// create statement object
					connectionStatement = conn.createStatement();
					System.out.println("Statement Created");
					
					// Attempts to create a table called employees, but one already exists so nothing gets committed
					
					connectionStatement.executeUpdate("CREATE TABLE employees (Id integer, First_Name varchar(15), Last_Name varchar(15), primary key(Id))");
					
					//Attempts to create a table called students, one does not already exit so it is created
					
					connectionStatement.executeUpdate("CREATE TABLE students (Id integer, First_Name varchar(15), Last_Name varchar(15), primary key(Id))");
					
					// Either the 2 following inserts are executed, or none of them are. This is atomicity.
     
					final String firstName = "Stacy"; 
					final String lastName = "Johnson";
					final String firstName2 = "John";
					final String lastName2 = "Davis";
					
					connectionStatement.executeUpdate("INSERT INTO employees (Employee_Id, FirstName, LastName, Age, Salary) VALUES (11111, '"+firstName+"','"+lastName+"', 20, 11111)");
					connectionStatement.executeUpdate("INSERT INTO employees (Employee_Id, FirstName, LastName, Age, Salary) VALUES (22215, '"+firstName2+"','"+lastName2+"', 29, 60000)");
					
					System.out.println("values added");
				} catch (SQLException e) {
					System.out.println("catch Exception");
					// For atomicity
					conn.rollback();
					connectionStatement.close();
					conn.close();
					return;
				} // main
				conn.commit();
				connectionStatement.close();
				conn.close();	
		
		
	}
}
