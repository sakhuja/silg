package ads.solr;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.logging.Logger;

import org.yaml.snakeyaml.Yaml;

public class SILG {    
    // default constants;
    private static String DRIVERNAME = "";
    private static String SERVERNAME = "";
    private static String PORT = "";
    private static String SID = "";
    private static String URL = "";
    private static String USERNAME = "";
    private static String PWD = "";

    //     private static String serverName = "ist6-db1-1-sfm-vip.data.sfdc.net";
    //     private static String portNumber = "1521";
    //     private static String sid = "ist6a176na1-1";


    private static Connection connection = null;
    private static Statement stmnt = null;

    private static Map<String,Integer> countMap = new HashMap<String,Integer>();
    private static Map<String,String> schemaMap = new HashMap<String,String>();
    private static Map<String,String> tableMap = new HashMap<String,String>();
    private static Logger silg_logger = null;

    public static void getConnection() {
        try {
            Class.forName(DRIVERNAME);
            connection = DriverManager.getConnection(URL, USERNAME, PWD);
            stmnt = connection.createStatement();
        } catch (SQLException e1) {
            e1.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void loadTableSizeMap(String namespaced_table) {
        
        FileInputStream fi = null;
        
        try {
            // This would lead to a NPE, if the table is 'namespaced_table' is missing. 
            File f = new File(System.getProperty("OUTPUT_PATH") + namespaced_table);
            f.createNewFile();
            fi = new FileInputStream(f);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        catch (IOException e) {
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

    /**
     * Getting size ( # of records ) for each nmspced_table
     */
    public static void storeTableSize() {
        String nmspced_table = "";
        String tablename = "";

        FileOutputStream fo = null;
        DataOutputStream out = null;
        BufferedWriter bw = null;

        System.out.println("schemaMap size : " + schemaMap.size());

        for (Map.Entry<String, String> entry : schemaMap.entrySet()) {

            // System.out.println(entry.getKey() + "/" + entry.getValue());
            // silg_logger.info(entry.getKey() + "/" + entry.getValue());
            tablename = tableMap.get(entry.getKey());
            System.out.println("tablename :" + tablename);

            if( tablename != null ){
                nmspced_table = entry.getValue() + "." + tablename;
                // silg_logger.info(nmspced_table);
                try {
                    String fn = System.getProperty("OUTPUT_PATH") + nmspced_table;                                                           
                    // silg_logger.info("fn:" + fn);
                    File f = new File(fn);
                    
                    if(f.exists()){
                        silg_logger.info("File already exists. not recreating..") ;
                        continue;
                    }
                    
                    fo = new FileOutputStream(f);
                    out = new DataOutputStream(fo);
                    bw = new BufferedWriter (new OutputStreamWriter(out));
                } catch (FileNotFoundException e1) {
                    e1.printStackTrace();
                } 
            }

            String getsize_query = "SELECT ORGANIZATION_ID, count(*) as CNT FROM " + nmspced_table + " GROUP BY ORGANIZATION_ID";

            try {
                silg_logger.info(getsize_query);
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
                bw.close();
                out.close();
                fo.close();
            } catch (SQLException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    /** 
     * Consumes process 'I' lines, in this format 
     * format  : <timestamp>, <indexingtype> , <entity_type_list> , <count>
     * example : 20111016000000.481,Add,00D30000000JizR,a0A,61 
     * 
     * @param threshold
     * @return
     */
    public static boolean processRecords(Integer threshold) {
        // format  : <timestamp>, <indexingtype> , <entity_type_list> , <count>	    
        String[] cols = new String[5];
        Random rg = new Random();
        String strLine;
        
        String load = System.getProperty("LOAD_FILE_CSV");        
        String source_csv = System.getProperty("SOURCE_FILE_CSV");

        try {			
            // silg_logger.info("Processing records from :");
            // silg_logger.info(System.getProperty("INPUT_PATH") + "/" + source_csv);
            FileInputStream fi = new FileInputStream(System.getProperty("INPUT_PATH") + "/" + source_csv);
            DataInputStream in = new DataInputStream(fi);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));

            // silg_logger.info("Writing org entity pairs to :" + System.getProperty("OUTPUT_PATH") + "/" + load);

            FileOutputStream fo = new FileOutputStream(System.getProperty("OUTPUT_PATH") + "/" + load);
            DataOutputStream out = new DataOutputStream(fo);
            BufferedWriter bw = new BufferedWriter (new OutputStreamWriter(out));			

            while ((strLine = br.readLine()) != null) {		    
                cols = strLine.split(",");
                if(cols.length != 5 ){
                    silg_logger.info("cols.length != 5.");
                    continue;
                }

                String ts = cols[0];
                // String indexing_type = cols[1];
                String org_id = cols[2];
                String keyPrefix = cols[3];
                Integer entity_size = Integer.parseInt(cols[4]);

                // System.out.println("Entity size :" + entity_size);

                if( entity_size > threshold ) {

                    if( keyPrefix.split(" ").length == 1 ) {
                        String dbschemaInfo = getPrefixInfo(keyPrefix,"D");
                        String tablenameInfo = getPrefixInfo(keyPrefix,"T");

                        // System.out.println("dbschemaInfo : " + dbschemaInfo + ", tablenameInfo : "  + tablenameInfo );

                        if(dbschemaInfo != null && tablenameInfo != null) {

                            String nmspced_table = dbschemaInfo + "." + tablenameInfo; 
                            loadTableSizeMap(nmspced_table);
                            String table_Id = tablenameInfo + "_ID";
                            // System.out.println("chkpoint 1");                            
                            Integer org_size = countMap.get(org_id);
                            Integer entity_count = null;
                            
                            try{
                              entity_count = org_size - entity_size - 1;
                            }catch(NullPointerException e){
                                silg_logger.info("org_size or entity_size is null.");
                                continue;
                            }                            
                            // System.out.println("entity_count : " + entity_count);
                            
                            if (entity_count <= 0 ){ 
                                silg_logger.info("entity_count was <= 0.");
                                continue;
                            }

                            Integer start_rownum = rg.nextInt(entity_count);
                            Integer stop_rownum = start_rownum + entity_size;

                            try {
                                String outer_query_start = "SELECT ORGANIZATION_ID," + table_Id + " FROM (";
                                String outer_query_end = ") tmp " + " WHERE rn > " + start_rownum.toString() + " AND rn <= " + stop_rownum.toString();
                                String inner_query = "SELECT ORGANIZATION_ID," + table_Id + ",rownum as rn FROM " + nmspced_table + " WHERE ORGANIZATION_ID ='" + org_id + "'";

                                System.out.println(outer_query_start + inner_query + outer_query_end);
                                // silg_logger.info("Running : " + outer_query_start + inner_query + outer_query_end);
                                ResultSet rs = stmnt.executeQuery(outer_query_start + inner_query + outer_query_end);

                                while(rs.next()) {
                                    // System.out.println("chkpoint 3");
                                    String orgid = rs.getString("ORGANIZATION_ID");
                                    String table_id = rs.getString(table_Id);
                                    // System.out.println(ts + "," + orgid + "," + table_id + "\n");
                                    bw.write(ts + "," + orgid + "," + table_id + "\n");                                    
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

    /**
     * Reading from dbschemas.map and tablesnames.map
     * and loading schemaMap and tableMap. 
     * 
     * @param mapfile
     * @param type
     */
    public static void populateMap(String mapfile,String type) {
        silg_logger.info("populating map...");
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
        // System.out.println("keyPrefix :" + keyPrefix);        
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

    private static void loadConfig() {

        silg_logger = Logger.getLogger("silg_logger");

        Yaml yaml = new Yaml();
        FileInputStream fstream = null;
        try {
            fstream = new FileInputStream("config/config.yaml");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        Map<String,String> yaml_map = (HashMap<String,String>) yaml.load(fstream);

        DRIVERNAME = yaml_map.get("DRIVERNAME");
        SERVERNAME= yaml_map.get("SERVERNAME");
        PORT = yaml_map.get("PORT");
        SID = yaml_map.get("SID");
        URL = "jdbc:oracle:thin:@" + SERVERNAME + ":" + PORT + ":" + SID;
        USERNAME = yaml_map.get("USERNAME");
        PWD = yaml_map.get("PWD");        
    
    }

    /**
     * Format results for incr indexing jsp
     * Input format : <timestamp>, <org_id>, <entity_id>
     * Output format : list<20 orgs> : list<20 entities>
     *
     * @throws IOException 
     */
    public static void format_results() throws IOException {
        
        String load = System.getProperty("LOAD_FILE_CSV");
        String grouped_load = System.getProperty("LOAD_FILE_GROUPED_CSV");
        
        FileInputStream fi = new FileInputStream(System.getProperty("OUTPUT_PATH") + "/" + load);
        DataInputStream in = new DataInputStream(fi);
        BufferedReader br = new BufferedReader (new InputStreamReader(in));

        FileOutputStream fo = new FileOutputStream(System.getProperty("OUTPUT_PATH") + "/" + grouped_load);
        DataOutputStream out = new DataOutputStream(fo);
        BufferedWriter bw = new BufferedWriter (new OutputStreamWriter(out));        

        String strLine = "";
        String cols[] = new String[3];
        List<String> org_entity = new ArrayList<String>();
        Integer entry_cnt = 0;
        
        while ((strLine = br.readLine()) != null) {
            
            cols = strLine.split(",");                       
            if(cols.length != 3 ) { 
                silg_logger.info(" cols.length != 3 ");
                continue;
            }
            
            org_entity.add(cols[1] + "," + cols[2]);            
            // silg_logger.info(cols[1]+ " , " + cols[2]);
            // silg_logger.info( "org_entity.size() : " + org_entity.size());
            
            
            if(org_entity.size() == 20) {
                // silg_logger.info("batch writing...");
                entry_cnt = 0; // used to track the position of the pairs currently being processed.
                for (String entry : org_entity){                    
                    entry_cnt++;                                        
                    bw.write(entry.split(",")[0]);
                    if(entry_cnt != 20) bw.write(",");                    
                }                               
                bw.write(":");                
                entry_cnt = 0; // resetting entries counter for the values 
                for (String entry : org_entity){                    
                    entry_cnt++;                                        
                    bw.write(entry.split(",")[1]);
                    if(entry_cnt != 20) bw.write(",");                    
                }                
                bw.write("\n");
                
                // empty the map, for next batch of 20 pairs
                // silg_logger.info("batch writing done.");
                org_entity.clear();           
            }
            
            silg_logger.info(" next line... \n");
            
        }

        // processing the tail of the stream... ( should be less than 20 entries )
        /* if(!org_entity.isEmpty()){
            
            for (int i=0;i<(org_entity.size()-1);i++){                  
                bw.write(org_entity.get(i).split(",")[0]);
                bw.write(",");
            }            
            bw.write(org_entity.get(org_entity.size()).split(",")[0]);
            
            bw.write(":");
            
            for (int i=0;i<(org_entity.size()-1);i++){                  
                bw.write(org_entity.get(i).split(",")[1]);
                bw.write(",");
            }            
            bw.write(org_entity.get(org_entity.size()).split(",")[1]);

            bw.write("\n");
        }*/

        br.close();
        in.close();
        fi.close();

        bw.close();
        out.close();
        fo.close();              

    }

    // entry point 
    public static void main(String args[]) {
        loadConfig();
        Integer threshold = Integer.parseInt(System.getProperty("THR"));
        Boolean flg_get_size = Boolean.parseBoolean(System.getProperty("GET_TABLE_SIZE"));

        silg_logger.info("threshold :" + threshold + " - " + "flg_get_size :" + flg_get_size + ".");

        populateMap(System.getProperty("INPUT_PATH") + "dbschemas.map","D");
        populateMap(System.getProperty("INPUT_PATH") + "tablesnames.map","T");

        getConnection();

        if(flg_get_size) {
            System.out.println("Calling store... ");
            storeTableSize(); 
        }

        processRecords(threshold);
        
        try {
            format_results();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}


