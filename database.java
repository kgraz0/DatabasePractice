import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

class database {
	
	public static void main(String[] args) throws IOException {

		String table = "markstable"; 
        Connection db = connectDb("com.mysql.jdbc.Driver","jdbc:mysql://igor.gold.ac.uk","kgraz001_jdbc","kgraz001","raketa3629?");
        Statement stmt = null;
        ArrayList<String> elem = new ArrayList<String>(); // create a new arraylist

        elem = readFile(); // store the returned arraylist

		try {
			stmt = db.createStatement();
			// delete the table if a table that name already exists
			stmt.executeUpdate("DROP TABLE IF EXISTS " + table);
			// create a new table with id, name, yearOfStudy and mark as column headers
			stmt.executeUpdate("CREATE TABLE " + table + " (id INT, name TEXT, yearOfStudy INT, mark INT, PRIMARY KEY (id))");
			
			// first part
			// loop through the arraylist and insert each line into the database using the INSERT INTO query
			for (int i = 1; i < elem.size(); i++) {
				stmt.executeUpdate("INSERT INTO " + table + " (" + elem.get(0) + ") VALUES (" + elem.get(i) + ")");
			}

            // second part (to display the data)
            //showData(table, db);

            // third part (to update Sofia's mark)
            //stmt.executeUpdate("UPDATE " + table + " SET mark = 100 WHERE name = 'Sofia O'");

            // fourth part (lowest and highest mark)
            /*
            for (int i = 1; i <= 3; i++) {
            	minMax("MAX", table, db, i);
            	minMax("MIN", table, db, i);
            	minMax("AVG", table, db, i);
            	System.out.println("\n");
            }
            */

			System.out.println("Query executed successfully.");
		    db.close();
		} catch (SQLException e) {
			System.out.println("Issue occurred when trying to execute query.");
			e.printStackTrace();
		}
	}

	private static Connection connectDb(String driver, String url, String database, String username, String password) {

		Connection conn = null;

		try {
			// load the database driver
			Class.forName(driver);

            // connection to the database
			conn = DriverManager.getConnection(url + "/" + database, username, password);
			System.out.println("Connected to database successfully.");

			 // catch ClassNotFound exception in case the driver name was not found
		} 	catch (ClassNotFoundException e) {
			System.out.println("Driver " + driver + " was not found.");
		// catch SQLException if there was an issue with the database connection
		}   catch (SQLException e) {
			System.out.println("Issue occurred with the connection to the database.");
		}

		return conn;
	}

	private static ArrayList<String> readFile() throws IOException {
		BufferedReader reader = null;
		ArrayList<String> elements = new ArrayList<String>();

        // try and catch block in case a file with that name does not exist
		try {
			reader = new BufferedReader(new FileReader("marks.txt"));
		} catch (FileNotFoundException e) {
			System.out.println("Specified file has not been found.");
		}

		String line = null;

        // read each line of the file, null is when the end of file is reached
		while ((line = reader.readLine()) != null) {
			line = line.replaceAll("\\s{2,}", ", "); // if there's more than 2 continuous white spaces, replace it with a comma
			// if there is a word, a space and another word, add single quote marks around it (helps when doing SQL query later)
			line = line.replaceAll("\\w+" + " " + "\\w", "'$0'"); 
			elements.add(line); // add the whole line to the arraylist 
		}
		reader.close(); // close the reader once file has been read

		return elements; // return the arraylist that contains lines from the file
	}

	private static void showData(String newTable, Connection newConnection) {

		Statement stmt = null;

        // try and catch block to catch any SQL Exceptions
		try {
			stmt = newConnection.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT * FROM " + newTable); // select everything from the table
			ResultSetMetaData rsmd = rs.getMetaData(); // used to get column headers
            
            // print out each column header from the database
			System.out.println(rsmd.getColumnName(1) + "\t\t  " + rsmd.getColumnName(2) + "\t\t  " + rsmd.getColumnName(3) + "\t\t  " + rsmd.getColumnName(4));

            // print data within each column from the result set 
			while (rs.next()) {
				System.out.println(rs.getInt(1) + "\t\t  " + rs.getString(2) + "\t\t" + rs.getInt(3) + "\t\t  " + rs.getInt(4));
			}

		} catch (SQLException e) {
			System.out.println("Issue occurred when trying to execute query.");
			e.printStackTrace();
		}
	}

    private static void minMax (String minMax, String newTable, Connection newConnection, int newYear) {

    	Statement stmt = null;

        // try and catch block to catch any SQL Exceptions
		try {
			stmt = newConnection.createStatement();
			// select either minimum, maximum or average mark from the table based on the given year
            ResultSet rs2 = stmt.executeQuery("SELECT " + minMax + "(mark) FROM " + newTable + " WHERE yearOfStudy = " + newYear);

            // print either minimum, maximum or average result
            while (rs2.next()) {
				System.out.println(minMax + " mark for year " + newYear + " : " + rs2.getInt(1));
			}

		} catch (SQLException e) {
			System.out.println("Issue occurred when trying to execute query.");
			e.printStackTrace();
		}

	}
}