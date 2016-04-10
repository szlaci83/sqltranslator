/**
 * API to handle Database connections and translate comma separated values(CSV) 
 * coming from the client to MYSQL statements, used by the server to query the 
 * database, and return the resultset as an ArrayList of appropriate objects.
 *
 *  @author Laszlo Szoboszlai
 *  @version 07/04/2016
 */

package server;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import common.*;

public class SQLTranslator2{

	//Default database's details:
	private final static String DEFAULT_PORT = ":3306"; 
	private final static String DEFAULT_DBURL = "jdbc:mysql://**********************";
	private final static String DEFAULT_USERNAME = "general";		
	private final static String DEFAULT_PASSWORD = "general";
	private final static String DRIVER = "com.mysql.jdbc.Driver"; // JDBC driver
	
	//flag to turn on test messages
	static boolean test = false;

	/**
	 *Creates ArrayList of CreditCard objects from a passed ResultSet.
	 */
	private static ArrayList<CreditCard> creditcardListBuilder(ResultSet rSet){
		ArrayList<CreditCard> returnList = new ArrayList<CreditCard>();
		try {
			while (rSet.next()) {
				CreditCard newCard = new CreditCard( rSet.getInt("cust_no"), rSet.getString("CARD_NO"), rSet.getString("start_date"),
						rSet.getString("exp_date"));
				returnList.add(newCard);
			}
		} catch (SQLException e) {
			System.out.println( "Error: problem with SQL resultset!" );
		}
		return returnList;
	}

	/**
	 *Creates ArrayList of Staff objects from a passed ResultSet.
	 */   
	private static ArrayList<Staff> staffListBuilder(ResultSet rSet){
		ArrayList<Staff> returnList = new ArrayList<Staff>();
		try {
			while (rSet.next()) {
				Staff newStaff = new Staff(rSet.getInt("STAFF_ID"),rSet.getString("first_name"),rSet.getString("last_name"));
				returnList.add(newStaff);
			}
		} catch (SQLException e) {
			System.out.println( "Error: problem with SQL resultset!" );
		}
		return returnList;
	}
	
	
	/**
	 *Creates ArrayList of Trip objects from a passed ResultSet.
	 */   
	private static ArrayList<Trip> tripListBuilder(ResultSet rSet){
		ArrayList<Trip> returnList = new ArrayList<Trip>();
		try {
			while (rSet.next()) {
				Trip newTrip = new Trip(rSet.getInt("TRIP_ID"),rSet.getInt("cust_id"),rSet.getInt("driver_id"),rSet.getString("from_address"),rSet.getString("from_city"),
						rSet.getString("from_county"),rSet.getString("from_postcode"),rSet.getString("to_address"),rSet.getString("to_city"),
						rSet.getString("to_county"),rSet.getString("to_postcode"),rSet.getString("trip_date"),rSet.getString("trip_time"));

				returnList.add(newTrip);
			}
		} catch (SQLException e) {
			System.out.println( "Error: problem with SQL resultset!" );
		}
		return returnList;
	}
	
	/**
	 *Creates ArrayList of Customer objects from a passed ResultSet.
	 */   
	private static ArrayList<Customer> customerListBuilder(ResultSet rSet){
		ArrayList<Customer> returnList = new ArrayList<Customer>();
		try {
			while (rSet.next()) {
				Customer newCustomer = new Customer(rSet.getInt("CUSTOMER_ID"),rSet.getString("first_name"),rSet.getString("last_name"),rSet.getString("address"),rSet.getString("city"),
						rSet.getString("county"),rSet.getString("postcode"),rSet.getString("email"),rSet.getString("phone_number"));
				returnList.add(newCustomer);
			}
		} catch (SQLException e) {
			System.out.println( "Error: problem with SQL resultset!" );
		}
		return returnList;
	}
	
	/**
	 *Creates ArrayList of Driver objects from a passed ResultSet.
	 */
	private static ArrayList<Driver> driverListBuilder(ResultSet rSet){
		ArrayList<Driver> returnList = new ArrayList<Driver>();
		try {
			while (rSet.next()) {
				Driver newDriver = new Driver(rSet.getInt("DRIVER_ID"), rSet.getString("first_name"), rSet.getString("last_name"),
						rSet.getString("licence_plate"),rSet.getString("phone_no"));
				returnList.add(newDriver);
			}
		} catch (SQLException e) {
			System.out.println( "Error: problem with SQL resultset!" );
		}
		return returnList;
	}

	/**
	 * Static method to make connection to the database.
	 * Details given in parameters.
	 */
	private static Connection connectToDB(String dataBase,String driver, String userName, String passWord ){
		Connection  dbConnection = null;
		try {
			//Register jdbc driver
			Class.forName(driver);
			// establish connection to database    
			dbConnection = DriverManager.getConnection(dataBase, userName, passWord);
			if ( test ) {
				System.out.println("Connected to DB!");
			}
			//return the database connection
			return dbConnection;		
		}catch (Exception exc) {
			System.out.println("Error: cannot connect to database!");
			return null;
		}
	}
	
	/**
	 * Static method to translate CSV to MYSQL statements.
	 * @param CSV : comma separated value as a command by the client.
	 * @return : MYSQL statement.
	 */
	//temporarily not private for testing 
	/*private*/ static String translateCSV(String CSV){
		StringBuilder sql = new StringBuilder();
		StringBuilder innerSQL;
		String CSVParts[] = CSV.split(",");
		switch (CSVParts[0].toUpperCase()){
		//CSV command: 
		//BOOK_TRIP,cust_id,from_address,from_city,from_county,from_postcode,to_address,to_city,to_county,to_postcode,trip_date,trip_time
		case "BOOK_TRIP" :
			sql.append("INSERT INTO trip (cust_id, from_address, from_city,"
					+ " from_county, from_postcode, to_address, to_city, to_county, to_postcode,"
					+ " trip_date, trip_time) VALUES (");
			sql.append(CSVParts[1]);
			sql.append(",");
			for(int i=2; i <CSVParts.length; i++){
				sql.append("'");
				sql.append(CSVParts[i]);
				sql.append("'");
				sql.append(",");	
			}
			//delete the last comma
			sql = sql.deleteCharAt(sql.length()-1);
			sql.append(");");
			break;
			//CSV command: 
			//VIEW_TRIP,cust_id
			//OR
			//VIEW_TRIP,driver_id
			//OR
			//VIEW_TRIP,NULL  (unassigned trips)
		case "VIEW_TRIP" : 	
			sql.append("SELECT * FROM trip WHERE ");
			int ID = 0;
			if ( ! CSVParts[1].equals("NULL") ){
			   ID = Integer.parseInt(CSVParts[1]);
			}
			if((ID >= 1000) && (ID < 3000)){sql.append("cust_id=");}
			if(ID >= 3000) {sql.append("driver_id=");}
			if(ID == 0) {sql.append("driver_id IS ");}
			sql.append(CSVParts[1]);
			sql.append(";");
			break;
			//CSV command: 
			//CANCEL_TRIP,trip_id
		case "CANCEL_TRIP" :
			sql.append( "DELETE FROM trip WHERE trip_id=" );
			sql.append(CSVParts[1]);
			sql.append(";");
			break;
			//CSV command:
			//ASSIGN_DRIVER,trip_id,driver_id
		case "ASSIGN_DRIVER" :
			sql.append("UPDATE trip SET driver_id=").append(CSVParts[2]);
			sql.append(" WHERE trip_id=").append(CSVParts[1]); 
			break;
			//CSV command:
			//SIGN_UP,first_name,last_name,address,county,city,postcode,email,phone_number
		case "SIGN_UP" :
			sql.append("INSERT INTO customer (first_name, last_name,"
					+ " address, county, city, postcode, email, phone_number)  VALUES( ");
			for(int i=1; i<CSVParts.length; i++){
				sql.append("'");
				sql.append(CSVParts[i]);
				sql.append("'");
				sql.append(",");	
			}
			//delete the last comma
			sql = sql.deleteCharAt(sql.length()-1);
			sql.append(");");
			break;
			//CSV command:
			//AVAILABLE_DRIVERS,trip_date,trip_time
		case "AVAILABLE_DRIVERS":
			innerSQL = new StringBuilder();
			sql.append("SELECT * FROM driver WHERE DRIVER_ID NOT IN ");
			//using a join to connect trips to driver details 
			innerSQL.append("(SELECT driver.DRIVER_ID FROM driver, trip WHERE driver.DRIVER_ID = "
					+ "trip.driver_id AND (trip.trip_date LIKE '");
			innerSQL.append(CSVParts[1]);
			innerSQL.append("' AND trip.trip_time LIKE '"); 
			innerSQL.append(CSVParts[2]);
			innerSQL.append("'));");
			sql.append(innerSQL);
			break;
		
			//CSV command:
			//LOGIN,userID,password
			//TODO : handle email login !!!!!!
		case "LOGIN":
			innerSQL = new StringBuilder();
			innerSQL.append("(SELECT ID FROM password  WHERE id=");
			innerSQL.append(CSVParts[1]);
			innerSQL.append(" AND ");
			innerSQL.append("pw LIKE '");
			innerSQL.append(CSVParts[2]);
			innerSQL.append("');");
			if (Integer.parseInt(CSVParts[1]) < 3000 ){
				sql.append("SELECT * FROM customer WHERE CUSTOMER_ID=");				
			}else{
			  if (Integer.parseInt(CSVParts[1]) < 6000){
				sql.append("SELECT * FROM driver WHERE DRIVER_ID=");
			  }
			  else{
				sql.append("SELECT * FROM company_staff WHERE STAFF_ID=");
			  }
			}
			sql.append(innerSQL);
			break;			
			//CSV command: 
			//GET_CARDS,userID
		case "GET_CARDS":
			sql.append( "SELECT * FROM card_info WHERE cust_id=" );
			sql.append(CSVParts[1]);
			break;
		}
		return sql.toString();	
	}
	
	/**
	 * The only public method, the class can be reached through to translate and execute 
	 * comma separated commands into SQL queries  and convert the ResultSet to an 
	 * ArrayList of relevant objects.
	 * @param CSV
	 * @return ArrayList created from the SQL ResultSet
	 */
	
		public static ArrayList execute(String CSV){
		Connection dbConnection = null;
		Statement SQLStatement = null;
		ResultSet SQLResultset = null;
		//the arraylist created from the resultset
		ArrayList returnList = new ArrayList();
		try {
			// establish connection to the default database    
			 dbConnection = SQLTranslator2.connectToDB(DEFAULT_DBURL, DRIVER, DEFAULT_USERNAME, DEFAULT_PASSWORD);
			 if ( test ) {
				 System.out.println("Creating statement objects!");
			 }
			//Create a statement object
			SQLStatement = dbConnection.createStatement();

			//translate the CSV:
			String SQLString = SQLTranslator2.translateCSV(CSV);
			if ( test ) {
				System.out.println("The created SQL query is: "  + SQLString);
			}
			//Execute SQL query
			SQLResultset = SQLStatement.executeQuery(SQLString);
			if ( test ) {
				System.out.println("The first column's name of the resultset: " + SQLResultset.getMetaData().getColumnName(1));
			}
			//Process the result set
			//if the resultset are  trips, we create a list of trips
			//creates  a list of trip objects from resultset
			if(SQLResultset.getMetaData().getColumnName(1).equals("TRIP_ID")){
				if ( test ) {
					System.out.println("Building triplist!");
				}
				returnList = tripListBuilder(SQLResultset);
			}
			//if the resultset are  drivers, we create a list of drivers
			//creates  a list of driver objects from resultset
			if (SQLResultset.getMetaData().getColumnName(1).equals("DRIVER_ID")){
				if ( test ) {
					System.out.println("building driver list!");
				}
				returnList = driverListBuilder(SQLResultset);
			}
			//if the resultset are  customers, we create a list of customers
			//creates  a list of customers objects from resultset
			if (SQLResultset.getMetaData().getColumnName(1).equals("CUSTOMER_ID")){
				if ( test ) {
					System.out.println("building customer list!");
				}
				returnList = customerListBuilder(SQLResultset); 
			}
			
			if (SQLResultset.getMetaData().getColumnName(2).equals("CARD_NO")){
				if ( test ) {
					System.out.println("building creditcard list!");
				}
				returnList = creditcardListBuilder(SQLResultset);
			}
			
			if (SQLResultset.getMetaData().getColumnName(1).equals("STAFF_ID")){
				if ( test ) {
					System.out.println("building staff list!");
				}
				returnList = staffListBuilder(SQLResultset);
			}
		}
		catch (Exception exc) {
			System.out.println( "There was an error during the SQL query!" );
		}
		//close everything down
		finally {
			if (SQLResultset != null) {
				try {
					SQLResultset.close();
				} catch (SQLException e) {
					System.out.println( "Error during closing the connection to the database!" );
				}
			}

			if (SQLStatement != null) {
				try {
					SQLStatement.close();
				} catch (SQLException e) {
					System.out.println( "Error: empty SQL statement!" );
				}
			}
			if (dbConnection != null) {
				try {
					dbConnection.close();
				} catch (SQLException e) {
					System.out.println("Error: no database to close!");
				}
			}
		}
		return returnList;	
	}	
}