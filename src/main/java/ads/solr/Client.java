

package ads.solr;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class Client {

	private static String driverName = "oracle.jdbc.driver.OracleDriver";
	private static String serverName = "YOURSERVERNAME";
	private static String portNumber = "1521";
	private static String sid = "SID";

	private static String url = "jdbc:oracle:thin:@" + serverName + ":" + portNumber + ":" + sid;
	private static String username = "";
	private static String password = "";
	private static Connection connection = null;
	private static Statement stmnt = null;

	private static Map<String,Integer> countMap = new HashMap<String,Integer>();
	private static Map<String,String> schemaMap = new HashMap<String,String>();
	private static Map<String,String> tableMap = new HashMap<String,String>();

	public static void getConnection() {
		try {
			Class.forName(driverName);
			connection = DriverManager.getConnection(url, username, password);
			stmnt = connection.createStatement();
		} catch (SQLException e1) {
			e1.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}


	public static void loadCountMap(String namespaced_table) {
		FileInputStream fi = null;
		try {
			
			File f = new File(System.getProperty("OUTPUT_PATH") + namespaced_table);
			
			fi = new FileInputStream(f);
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		DataInputStream in = new DataInputStream(fi);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String strLine = null;
		String[] cols = new String[2];

		try {
			while ((strLine = br.readLine()) != null) {
				cols = strLine.split(",");
				countMap.put(cols[0], Integer.parseInt(cols[1]));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	
	}
	

	public static void populateTableSize() {
		String nmspced_table = "";
		String tablename = "";

		FileOutputStream fo = null;
		DataOutputStream out = null;
		BufferedWriter bw = null;
		try {
			fo = new FileOutputStream(System.getProperty("OUTPUT_PATH") + nmspced_table);
			out = new DataOutputStream(fo);
			bw = new BufferedWriter (new OutputStreamWriter(out));
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		}

		for (Map.Entry<String, String> entry : schemaMap.entrySet()) {
			System.out.println(entry.getKey() + "/" + entry.getValue());
			tablename = tableMap.get(entry.getKey());
			System.out.println("tablename :" + tablename);
			if( tablename != null ){
				nmspced_table = entry.getValue() + "." + tablename;
				System.out.println(nmspced_table);
			}
			String getsize_query = "SELECT ORGANIZATION_ID, count(*) as CNT FROM " + nmspced_table + " GROUP BY ORGANIZATION_ID";
			try {
				System.out.println(getsize_query);
				ResultSet rs = stmnt.executeQuery(getsize_query);
				while(rs.next()) {
					String org = rs.getString("ORGANIZATION_ID");
					String cnt = rs.getString("CNT");
					String line = org + "," + cnt + "\n";
					// System.out.println(line);
					try {
						bw.write(line);
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

		try {
			bw.close();
			out.close();
			fo.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static boolean processRecords(Integer threshold) {

		String[] cols = new String[4];
		Random rg = new Random();
		String strLine;

		try {
			FileInputStream fi = new FileInputStream(System.getProperty("INPUT_PATH") + "/20_Ilines.txt");
			DataInputStream in = new DataInputStream(fi);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));

			FileOutputStream fo = new FileOutputStream(System.getProperty("OUTPUT_PATH") + "/orgs_entities.lst");
			DataOutputStream out = new DataOutputStream(fo);
			BufferedWriter bw = new BufferedWriter (new OutputStreamWriter(out));

			while ((strLine = br.readLine()) != null) {
				cols = strLine.split(",");

				if(cols.length != 4) continue;

				String ts = cols[0];
				String org_id = cols[1];
				String keyPrefix = cols[2];
				Integer entity_size = Integer.parseInt(cols[3]);

				if( entity_size > threshold ) {

					if( keyPrefix.split(" ").length == 1 ) {
						String dbschemaInfo = getPrefixInfo(keyPrefix,"D");
						String tablenameInfo = getPrefixInfo(keyPrefix,"T");
						if(dbschemaInfo!=null && tablenameInfo!=null) {
							String nmspced_table = dbschemaInfo + "." + tablenameInfo; 
							loadCountMap(nmspced_table);
							String table_Id = tablenameInfo + "_ID";

							Integer org_size = countMap.get(org_id);
							Integer entity_count = org_size - entity_size - 1;
							Integer start_rownum = rg.nextInt(entity_count);
							Integer stop_rownum = start_rownum + entity_size;

							try {
								String outer_query_start = "SELECT ORGANIZATION_ID," + table_Id + " FROM (";
								String outer_query_end = ") tmp " + " WHERE rn > " + start_rownum.toString() + " AND rn <= " + stop_rownum.toString();
								String inner_query = "SELECT ORGANIZATION_ID," + table_Id + ",rownum as rn FROM " + nmspced_table + " WHERE ORGANIZATION_ID ='" + org_id + "'";
								
								// System.out.println(outer_query_start + inner_query + outer_query_end);
								
								ResultSet rs = stmnt.executeQuery(outer_query_start + inner_query + outer_query_end);
								
								while(rs.next()) {
									String org = rs.getString("ORGANIZATION_ID");
									String table_id = rs.getString(table_Id); 
									bw.write(ts + "," + org + "," + table_id + "\n");
								}
							} catch (SQLException e) {
								e.printStackTrace();
							}
						}
					}
				}
			}

			bw.close();
			out.close();
			fo.close();
			br.close();
			in.close();
			fi.close();
		}
		catch (Exception e){
			e.printStackTrace();
		}
		return true;
	}

	public static void populateMap(String mapfile,String type) {
		try{
			FileInputStream fstream = new FileInputStream(mapfile);
			DataInputStream in = new DataInputStream(fstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String strLine;
			String[] cols = new String[2];

			while ((strLine = br.readLine()) != null) {
				cols = strLine.split(",");
				if(cols.length == 2){

					if(type.equals("D")){
						schemaMap.put(cols[0], cols[1]);
					}
					else{
						if(type.equals("T")){
							// System.out.println(cols[0] + "," + cols[1]);
							tableMap.put(cols[0], cols[1]);
						}
					}
				}
			}
			// System.out.println (strLine);
			br.close();
			in.close();
			fstream.close();
		}
		catch (Exception e){
			e.printStackTrace();
		}
	}

	public static String getPrefixInfo(String keyPrefix, String type) {
		if(type.equals("T")){
			if(tableMap.containsKey(keyPrefix)){
				return tableMap.get(keyPrefix); 
			} 
		}
		
		if(type.equals("D")) {
			if(schemaMap.containsKey(keyPrefix)){
				return schemaMap.get(keyPrefix); 
			} 
		}
		
		return null;
	}

	// entry point 
	public static void main(String args[]) {
		
		Integer threshold = Integer.parseInt(System.getProperty("THR"));
		Boolean flg_get_size = Boolean.parseBoolean(System.getProperty("GET_TABLESIZE"));
		
		System.out.println("threshold :" + threshold + " - " + "flg_get_size :" + flg_get_size + ".");
		
		populateMap(System.getProperty("INPUT_PATH") + "dbschemas.map","D");
		populateMap(System.getProperty("INPUT_PATH") + "tablesnames.map","T");
		
		getConnection();
		if(flg_get_size) { populateTableSize(); }
		processRecords(threshold);
	}
	
}
