import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterAll;

public class TestAmazonRedshift 
{
	/**
	 * Class being tested
	 */
	private static AmazonRedshift q;
	
	/**
	 * Connection to the database
	 */
	private static Connection con;
	

	/**
	 * Requests a connection to the database.
	 * 
	 * @throws Exception
	 * 		if an error occurs
	 */
	@BeforeAll
	public static void init() throws Exception 
	{		
		q = new AmazonRedshift();
		con = q.connect();					
	}
	
	/**
	 * Closes connection.
	 * 
	 * @throws Exception
	 * 		if an error occurs
	 */
	@AfterAll
	public static void end() throws Exception 
	{
		q = new AmazonRedshift();
		q.close();
        
	}
	
	/**
     * Tests SSL connection
     */
    @Test
    public void testConnect() 
    {   
		try
    	{ 	
			Connection c = q.connect();
			if (c == null)
				fail("Failed to connect");
		}
		catch (SQLException e)
		{
			System.out.println(e);
			fail("Connection failed");
		}		
    }

	/**
     * Tests drop command.
     */
    @Test
    public void testDrop() 
    {    		
    	//q.drop();
    	
		if (con == null)
			fail("FAIL: No connection");			

    	// See if tables exist
		String []tables = new String[]{"region", "nation", "part", "supplier", "partsupp", "customer", "orders", "lineitem"};
		for (int i=0; i < tables.length; i++)
		{
			try
			{	    	
				Statement stmt = con.createStatement();			
	    		stmt.executeQuery("SELECT * FROM "+tables[i]);
	    		fail("Table "+tables[i]+" exists and should be dropped!");			
			}
			catch (SQLException e)
			{
				System.out.println(e);			
			}
		}
    }

    
    /**
     * Tests insert.
     */
    @Test
    public void testInsert() throws SQLException
    {    

    	// See if tables exists
    	try
    	{
			// System.out.println(System.getProperty("user.dir"));
			//q.insert();
			if (con == null)
				fail("FAIL: No connection");			
	    	Statement stmt = con.createStatement();						
	    	assertEquals(5, getFirstRowValue(stmt, "SELECT COUNT(*) FROM region"));
			assertEquals(25, getFirstRowValue(stmt, "SELECT COUNT(*) FROM nation"));
			assertEquals(2000, getFirstRowValue(stmt, "SELECT COUNT(*) FROM part"));
			assertEquals(100, getFirstRowValue(stmt, "SELECT COUNT(*) FROM supplier"));
			assertEquals(1500, getFirstRowValue(stmt, "SELECT COUNT(*) FROM customer"));
			assertEquals(8000, getFirstRowValue(stmt, "SELECT COUNT(*) FROM partsupp"));
			assertEquals(15000, getFirstRowValue(stmt, "SELECT COUNT(*) FROM orders"));
			assertEquals(60005, getFirstRowValue(stmt, "SELECT COUNT(*) FROM lineitem"));						
			System.out.println("Insert successful");
    	}
    	catch (SQLException e)
    	{
    		System.out.println(e);
			fail(e.toString());
    	}
    }
    
    
    /**
     * Tests first query.
     */
    @Test
    public void testQuery1() throws SQLException
    {    

    	ResultSet rst = q.query1();
    	
    	// Verify result
		String answer = "Total columns: 3"
						+"\nl_orderkey, totalsale, o_orderdate"
						+"\n7104, 25969, 2018-12-31"
						+"\n32775, 266291, 2018-12-31"
						+"\n43299, 35816, 2018-12-29"
						+"\n44551, 25577, 2018-12-27"
						+"\n26242, 239214, 2018-12-27"
						+"\n27971, 237166, 2018-12-27"
						+"\n32518, 78182, 2018-12-27"
						+"\n33412, 41583, 2018-12-26"
						+"\n28615, 202064, 2018-12-25"
						+"\n43077, 65839, 2018-12-25"						
						+"\nTotal results: 10";
    	String queryResult = AmazonRedshift.resultSetToString(rst, 100);
    	System.out.println(queryResult);
    	assertEquals(answer, queryResult);   
    }
    
    /**
     * Tests second query.
     */
    @Test
    public void testQuery2() throws SQLException
    {      	
    	
    	ResultSet rst = q.query2();
    	
    	// Verify result
    	String answer = "Total columns: 2"
						+"\nc_custkey, sum"
						+"\n962, 922160"
						+"\n1052, 828764"
						+"\n103, 755473"
						+"\n1061, 729966"
						+"\n1279, 724422"
						+"\n664, 645318"
						+"\n1331, 636010"
						+"\n1415, 617007"
						+"\n334, 609507"
						+"\n1144, 603939"
						+"\n1316, 594293"
						+"\n1334, 588675"
						+"\n1345, 581213"
						+"\n340, 569714"
						+"\n1013, 552736"
						+"\n1027, 537989"
						+"\n694, 530579"
						+"\n1253, 527909"
						+"\n818, 518624"
						+"\n229, 514761"
						+"\n1124, 513362"
						+"\n835, 511518"
						+"\n380, 505081"
						+"\n575, 502321"
						+"\n1214, 502168"
						+"\n1268, 479494"
						+"\n188, 469048"
						+"\n995, 446382"
						+"\n932, 444419"
						+"\n767, 443258"
						+"\n134, 434497"
						+"\n1486, 431676"
						+"\n1075, 428321"
						+"\n329, 415428"
						+"\n512, 411627"
						+"\n1, 409276"
						+"\n649, 401721"
						+"\n662, 398527"
						+"\n674, 395791"
						+"\n508, 392079"
						+"\n844, 389389"
						+"\n814, 377757"
						+"\n1223, 374731"
						+"\n1046, 369845"
						+"\n803, 365192"
						+"\n592, 363997"
						+"\n938, 358816"
						+"\n185, 355469"
						+"\n709, 353422"
						+"\n968, 352454"
						+"\n1414, 352023"
						+"\n553, 345776"
						+"\n580, 343227"
						+"\n298, 329493"
						+"\n1163, 324104"
						+"\n1009, 322159"
						+"\n1100, 322034"
						+"\n568, 316454"
						+"\n1115, 313948"
						+"\n805, 312105"
						+"\n1433, 307089"
						+"\n1295, 302681"
						+"\n1202, 302515"
						+"\n1430, 299223"
						+"\n1400, 298761"
						+"\n1091, 298554"
						+"\n1237, 296683"
						+"\n860, 295433"
						+"\n77, 292749"
						+"\n220, 288443"
						+"\n1235, 285691"
						+"\n1453, 284024"
						+"\n152, 281777"
						+"\n610, 274345"
						+"\n986, 273984"
						+"\n476, 273240"
						+"\n722, 265953"
						+"\n73, 251451"
						+"\n116, 247189"
						+"\n428, 244923"
						+"\n1318, 243849"
						+"\n223, 240793"
						+"\n1405, 240775"
						+"\n475, 239713"
						+"\n944, 238381"
						+"\n1475, 237042"
						+"\n1201, 236796"
						+"\n211, 235013"
						+"\n40, 234234"
						+"\n1208, 232416"
						+"\n790, 231147"
						+"\n523, 230369"
						+"\n280, 229087"
						+"\n392, 227033"
						+"\n1183, 216192"
						+"\n98, 213476"
						+"\n1312, 211671"
						+"\n1460, 211434"
						+"\n400, 207240"
						+"\n653, 200190"
						+"\nTotal results: 166";
	
    	String queryResult = AmazonRedshift.resultSetToString(rst, 100);
    	System.out.println(queryResult);
    	assertEquals(answer, queryResult);   
    }
    
    /**
     * Tests third query.
     */
    @Test
    public void testQuery3() throws SQLException
    {    
 	
    	ResultSet rst = q.query3();
    	
    	// Verify result
    	String answer = "Total columns: 2"
						+"\no_orderpriority, order_count"
						+"\n1-URGENT       , 1387"
						+"\n2-HIGH         , 1303"
						+"\n3-MEDIUM       , 1287"
						+"\n4-NOT SPECIFIED, 1530"
						+"\n5-LOW          , 1268"
						+"\nTotal results: 5";
	
				
    	String queryResult = AmazonRedshift.resultSetToString(rst, 100);
    	System.out.println(queryResult);
    	assertEquals(answer, queryResult);   
    }    
    
    /**
     * Runs an SQL query and compares answer to expected answer.  
     * 
     * @param sql
     * 		SQL query
     * @param answer
     * 		expected answer          
     */
    public static void runSQLQuery(String sql, String answer)
    {    	 
         try
         {
        	Statement stmt = con.createStatement();
 	    	ResultSet rst = stmt.executeQuery(sql);	    	
 	    	
 	    	String st = AmazonRedshift.resultSetToString(rst, 1000);
 	    	System.out.println(st);	    			
 	    		
 	    	assertEquals(answer, st);	           	             
            
 	    	stmt.close();
         }            
         catch (SQLException e)
         {	
        	 System.out.println(e);
        	 fail("Incorrect exception: "+e);
         }              
    }

	public int getFirstRowValue(Statement stmt, String query) throws SQLException
	{
		ResultSet rs = stmt.executeQuery(query);
		rs.next();
		return rs.getInt(1);
	}
}
