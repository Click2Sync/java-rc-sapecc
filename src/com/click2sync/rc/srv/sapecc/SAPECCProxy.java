package com.click2sync.rc.srv.sapecc;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import com.sap.conn.jco.AbapException;
import com.sap.conn.jco.JCoDestination;
import com.sap.conn.jco.JCoDestinationManager;
import com.sap.conn.jco.JCoException;
import com.sap.conn.jco.JCoExtendedFieldMetaData;
import com.sap.conn.jco.JCoField;
import com.sap.conn.jco.JCoFunction;
import com.sap.conn.jco.JCoParameterFieldIterator;
import com.sap.conn.jco.JCoParameterList;
import com.sap.conn.jco.JCoRecordMetaData;
import com.sap.conn.jco.JCoStructure;
import com.sap.conn.jco.JCoTable;
import com.sap.conn.jco.ext.DestinationDataProvider;

public class SAPECCProxy {
	
	Properties config;
	
	static String ABAP_AS = "ABAP_AS_WITHOUT_POOL";
    static String ABAP_AS_POOLED = "ABAP_AS_WITH_POOL";
    static String ABAP_MS = "ABAP_MS_WITHOUT_POOL";

    static String DESTINATION_NAME = "ABAP_AS_WITHOUT_POOL";
    
	static final Pattern VALID_EMAIL_ADDRESS_REGEX = Pattern.compile("^[A-Z0-9._%+-]+@[A-Z0-9.-]+\\.[A-Z]{2,6}$", Pattern.CASE_INSENSITIVE);
	Main delegate;
	
	JCoTable materials;
	JCoTable orders;
	
	JCoDestination destination;
	
	public int currentmaterial = 0;
	public int currentorder = 0;
	public int offsetproducts = 0;
	public int offsetorders = 0;
	
	public SAPECCProxy(Main main) {
		delegate = main;
	}

	public void setProperties(Properties props) {
		config = props;
	}
	
	private void connect() throws NoSAPECCException {

		try {
			//SAP stuff
			createPropertiesForConnection();
			
		} finally {
			
		}
		
	}
	
	public void sense() throws NoSAPECCException {
		
		ServiceLogger.log("Sensing environment for SAP ECC...");
		connect();
		
	}
	
	public void setProductsStreamCursor(Long offset) throws NoSAPECCException {

		currentmaterial = 0;
		Date date = new Date(offset);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		String dateFormatted = sdf.format(date);
		
		String fields[] = {"MATNR", "LAEDA"};
		materials = this.consultSapTable("MARA", ";", "0", "3000", "LAEDA > '"+dateFormatted+"'", fields);
		materials.setRow(0);
//		String query = "SELECT * FROM dbo.OITM WHERE UpdateDate >= ? ORDER BY UpdateDate ASC";
//		String overridequery = delegate.config.getProperty("filterquerysqloverride");
//		if(overridequery != null && overridequery.length() > 0) {
//			query = overridequery;
//		} 20060613
//		query.replaceFirst("\\?", "'"+dateFormatted+"'");
		
	}
	
	public boolean hasMoreProducts(Long offsetdate) throws NoSAPECCException {
		
		if(currentmaterial >= materials.getNumRows()) {
			return false;
//			ServiceLogger.log("appears to be all products, lets check");
//			this.setProductsStreamCursor(offsetdate);
//			if(materials.getNumRows() > 0) {
//				ServiceLogger.log("nope, there are more products");
//				return true;
//			} else {
//				ServiceLogger.log("yes, all products done");
//				return false;
//			}
		} else {
			return true;
		}
		
	}
	
	public JSONObject nextProduct() throws NoSAPECCException {
		JSONObject product = convertSAPProductToC2SProduct();
		currentmaterial = currentmaterial + 1;
		if(currentmaterial >= materials.getNumRows()) {
			materials.setRow(currentmaterial);;
		}
		return product;
		
	}
	
	public void setOrdersStreamCursor(Long offset) throws NoSAPECCException {
		
		currentorder = 0;
		Date date = new Date(offset);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		String dateFormatted = sdf.format(date);
		
		String fields[] = {"VBELN", "KUNNR", "ERDAT"};
		orders = this.consultSapTable("VBAK", ";", "0", "3000", "ERDAT > '"+dateFormatted+"'", fields);
		orders.setRow(0);

	}
	
	public boolean hasMoreOrders(Long offsetdate) throws NoSAPECCException {
		if(currentorder >= orders.getNumRows()) {
			return false;
//			ServiceLogger.log("appears to be all products, lets check");
//			this.setProductsStreamCursor(offsetdate);
//			if(materials.getNumRows() > 0) {
//				ServiceLogger.log("nope, there are more products");
//				return true;
//			} else {
//				ServiceLogger.log("yes, all products done");
//				return false;
//			}
		} else {
			return true;
		}

	}
	
	public JSONObject nextOrder() throws NoSAPECCException {
		JSONObject order = convertSAPOrderToC2SOrder();
		currentorder = currentorder + 1;
		if(currentorder >= orders.getNumRows()) {
			orders.setRow(currentorder);;
		}
		return order;
		
	}

	public JSONObject storeProduct(JSONObject product) throws NoSAPECCException {
		
		String productid = (String) product.get("_id");
		boolean isInsert = false;
		if(isInsert) {
			throw new NoSAPECCException("SAPB1 is protected, the implementation restricts new products on SAP... ");
		} else {
			throw new NoSAPECCException("SAPB1 is protected, the implementation restricts product updates on SAP... ");
			//products.getByKey(productid);
			//return convertSAPProductToC2SProduct(products);
		}
		
	}
	
	@SuppressWarnings("unchecked")
	public JSONObject storeOrder(JSONObject order) throws NoSAPECCException {
		
		//extract data from external order object
		String orderid = (String) order.get("_id");
		String orderconnectiontype = (String) order.toJSONString();
		JSONArray orderItems = (JSONArray) order.get("orderItems");
		JSONObject buyer = (JSONObject) order.get("buyer");
		String email = (String) buyer.get("email");
		String buyerid = Long.toString((Long) buyer.get("id"));
		String refidstr = "";
		
		//get from where this order originally comes from
		try {
			JSONArray referenceids = (JSONArray) order.get("otherids");
			JSONObject referenceid = (JSONObject) referenceids.get(0);
			refidstr = (String) referenceid.get("id");
			String conntype = (String) referenceid.get("connectiontype");
			String sufix = returnSufixForRefId(conntype);
			refidstr = sufix + refidstr;
		}catch(Exception e) {
			ServiceLogger.error(new C2SRCServiceException("Could not extract referenceid from online order. "+e.getMessage()));
		}
		
		boolean isInsert = false;
		int orderdockey = -1;
		try {
			orderdockey = Integer.parseInt(orderid);
			
		}catch(NumberFormatException e) {//we have something like tmp-ordidblablabla
			isInsert = true;
		}
		
		if(isInsert) {
			try {

		        //check if properties file exists if not create one and initialize destination
		        loadPropertiesForConnection();
		        destination = JCoDestinationManager.getDestination("./"+DESTINATION_NAME);
		        
		        //needs transactionid to correctly add a new order in commit
		        String newTransactionId = destination.createTID();
		        
		        //get functions to execute later
		        JCoFunction createorderfunction = destination.getRepository().getFunction("BAPI_SALESORDER_CREATEFROMDAT2");
		        JCoFunction commitfunction = destination.getRepository().getFunction("BAPI_TRANSACTION_COMMIT");
		        JCoParameterList createorderparamlist = createorderfunction.getImportParameterList();

		        //construct structure for header before execute
		        JCoStructure sohdrstructure = createorderparamlist.getStructure("ORDER_HEADER_IN");
		        sohdrstructure.setValue("DOC_TYPE", "TA");
		        sohdrstructure.setValue("SALES_ORG", "1001");
		        sohdrstructure.setValue("DISTR_CHAN", "");
		        sohdrstructure.setValue("DIVISION", "");

		        //add order items
		        JCoTable saleorderitemstable = createorderfunction.getTableParameterList().getTable("ORDER_ITEMS_IN");
		        for (int i = 0; i < orderItems.size(); i++) {
		        	JSONObject orderItem = (JSONObject) orderItems.get(i);
		        	String itemcode = (String) orderItem.get("id");
		        	saleorderitemstable.appendRow();
		        	saleorderitemstable.setValue("MATERIAL", itemcode);
		        }

		        //Set values of ORDER_PARTNERS table
		        JCoTable saleorderpartnerstable = createorderfunction.getTableParameterList().getTable("ORDER_PARTNERS");
		        //Get specific partner for details need info for ORDER_PARTNERS
		        //Obtain client details
		        String fieldscustomerdetails[] = {"KUNNR", "NAME1", "ORT01", "PSTLZ", "REGIO", "LAND1", "STRAS", "TELF1"};
		        String customerid="", customername="", customercity="", customerzip="", customerregion="", customercountry="", customeraddress="", customertelephone="";		
		        JCoTable tablecustomer = this.consultSapTable("KNA1", ";", "0", "1", "KUNNR = '"+delegate.config.getProperty("defaultcustomerid")+"'", fieldscustomerdetails);
		        if(tablecustomer.getNumRows() > 0) {
		            String[] customerline = tablecustomer.getString(0).split(";");
		            customerid = customerline[0];
		            customername = customerline[1];
		            customercity = customerline[2];
		            customerzip = customerline[3];
		            customerregion = customerline[4];
		            customercountry = customerline[5];
		            customeraddress = customerline[6];
		            if(customerline.length > 7)
		                customertelephone = customerline[7];
		        }
		        
		        saleorderpartnerstable.appendRow();
		        saleorderpartnerstable.setValue("PARTN_ROLE", "WE");
		        saleorderpartnerstable.setValue("PARTN_NUMB", delegate.config.getProperty("defaultcustomerid"));
		        saleorderpartnerstable.setValue("ITM_NUMBER", "000000");
		        saleorderpartnerstable.setValue("NAME", customername);
		        saleorderpartnerstable.setValue("STREET", customeraddress);
		        saleorderpartnerstable.setValue("COUNTRY", customercountry);
		        saleorderpartnerstable.setValue("POSTL_CODE", customerzip);
		        saleorderpartnerstable.setValue("CITY", customercity);
		        saleorderpartnerstable.setValue("TELEPHONE", customertelephone);

		        if(createorderfunction == null)
		            throw new RuntimeException("BAPI_SALESORDER_CREATEFROMDAT2 not found in SAP.");
		            System.out.println();
		        try {
		            createorderfunction.execute(destination, newTransactionId);
		            System.out.println("return createorder: "+createorderfunction.getTableParameterList().getTable("RETURN"));

		        } catch(AbapException e) {
		            System.out.println(e.toString());
		        } finally {
		            try {
		                commitfunction.execute(destination);
		                commitfunction.execute(destination, newTransactionId);
		                System.out.println("return commit: "+commitfunction.getExportParameterList().getValue("RETURN"));

		            } catch(AbapException e) {
		                System.out.println(e.toString());
		            }	
		        }
				
			} catch(Exception e) {
				
			}
			//start building store request to SAPB1
			return null;
			
		} else {//is update
			
			return null;
			
		}
		
		
	}
	
	//TODO properties file destinationName+".jcoDestination" con datos
	public void test_read_props_file() throws NoSAPECCException {
		
		
		
	}
	
	//TODO test function get a list of orders of SAP instance
	public void test_get_orders2() throws NoSAPECCException {
		PrintStream out;
		try {
			out = new PrintStream(new FileOutputStream("outputsap.txt"));
			System.setOut(out);
		} catch (FileNotFoundException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		
		//get list of all customers
		try {
			
			//check if properties file exists if not create one
			loadPropertiesForConnection();
			
			JCoDestination destination = JCoDestinationManager.getDestination("./"+DESTINATION_NAME);
			
			JCoFunction function = destination.getRepository().getFunction("BAPI_CUSTOMER_GET_ROOT_LIST");
			JCoParameterList paramlist = function.getImportParameterList();
			
	        if(function == null)
	            throw new RuntimeException("BAPI_MATERIAL_GETLIST not found in SAP.");
	        	System.out.println();
	        try {
	            function.execute(destination);
	        } catch(AbapException e) {
	            System.out.println(e.toString());
	            return;
	        }
	        
	        JCoTable codes = function.getTableParameterList().getTable("NODE_LIST");
	        
	        System.out.println("return: "+function.getExportParameterList().getValue("RETURN"));
	        System.out.println("Num rows: "+codes.getNumRows()+"  Num cols: "+codes.getNumColumns());
	        for(int i = 0; i<codes.getNumRows(); i++) {
	        	codes.setRow(i);
	        	//System.out.println("AN: "+codes.getString("ABAPNAME")+" BN: "+codes.getString("BAPINAME")+" T: "+codes.getString("BAPI_TEXT"));
	        }
		} catch (JCoException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		//get orders list of one customer
		try {
			
			//check if properties file exists if not create one
			loadPropertiesForConnection();
			
			JCoDestination destination = JCoDestinationManager.getDestination("./"+DESTINATION_NAME);
			
			
			JCoFunction function = destination.getRepository().getFunction("BAPI_SALESORDER_GETLIST");
			JCoParameterList paramlist = function.getImportParameterList();
			paramlist.setValue("CUSTOMER_NUMBER", "0000100013");
			paramlist.setValue("SALES_ORGANIZATION", "1000");
			
	        if(function == null)
	            throw new RuntimeException("BAPI_MATERIAL_GETLIST not found in SAP.");
	        	System.out.println();
	        try {
	            function.execute(destination);
	        } catch(AbapException e) {
	            System.out.println(e.toString());
	            return;
	        }
	        
	        JCoTable codes = function.getTableParameterList().getTable("SALES_ORDERS");
	        
	        System.out.println("return: "+function.getExportParameterList().getValue("RETURN"));
	        System.out.println("Num rows: "+codes.getNumRows()+"  Num cols: "+codes.getNumColumns());
	        for(int i = 0; i<codes.getNumRows(); i++) {
	        	codes.setRow(i);
	        	//System.out.println("AN: "+codes.getString("ABAPNAME")+" BN: "+codes.getString("BAPINAME")+" T: "+codes.getString("BAPI_TEXT"));
	        }
		} catch (JCoException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	//TODO test function to create a new sale order to SAP instance
		public void test_get_orders() throws NoSAPECCException {
			PrintStream out;
			try {
				out = new PrintStream(new FileOutputStream("outputsap.txt"));
				System.setOut(out);
			} catch (FileNotFoundException e2) {
				// TODO Auto-generated catch block
				e2.printStackTrace();
			}
			
			//create a new sales order
			try {
				
				//check if properties file exists if not create one
				loadPropertiesForConnection();
				
				destination = JCoDestinationManager.getDestination("./"+DESTINATION_NAME);
				
				String newTransactionId = destination.createTID();
				
				JCoFunction createorderfunction = destination.getRepository().getFunction("BAPI_SALESORDER_CREATEFROMDAT2");
				JCoFunction commitfunction = destination.getRepository().getFunction("BAPI_TRANSACTION_COMMIT");
				JCoParameterList createorderparamlist = createorderfunction.getImportParameterList();
				
				
				//construct structure for header before execute
				JCoStructure sohdrstructure = createorderparamlist.getStructure("ORDER_HEADER_IN");
				System.out.println("Table: ORDER_HEADER_IN");
				sohdrstructure.setValue("DOC_TYPE", "TA");
				System.out.println("DOC_TYPE: " + "TA");
				sohdrstructure.setValue("SALES_ORG", "1001");
				System.out.println("SALES_ORG: " + "1001");
				sohdrstructure.setValue("DISTR_CHAN", "");
				System.out.println("DISTR_CHAN: " + "");
				sohdrstructure.setValue("DIVISION", "");
				System.out.println("DIVISION: " + "");
				
				//additional input for sales order
				JCoTable saleorderitemstable = createorderfunction.getTableParameterList().getTable("ORDER_ITEMS_IN");
				System.out.println("Table: ORDER_ITEMS_IN and appendRow");
				saleorderitemstable.appendRow();
				saleorderitemstable.setValue("MATERIAL", "000000000300000539");
				System.out.println("MATERIAL: " + "000000000300000539");
				
				//Set values of ORDER_PARTNERS table
				JCoTable saleorderpartnerstable = createorderfunction.getTableParameterList().getTable("ORDER_PARTNERS");
				//Get specific partner for details need info for ORDER_PARTNERS
				//Obtain client details
				String fieldscustomerdetails[] = {"KUNNR", "NAME1", "ORT01", "PSTLZ", "REGIO", "LAND1", "STRAS", "TELF1"};
				String customerid="", customername="", customercity="", customerzip="", customerregion="", customercountry="", customeraddress="", customertelephone="";		
				JCoTable tablecustomer = this.consultSapTable("KNA1", ";", "0", "1", "KUNNR = '"+"0000100013"+"'", fieldscustomerdetails);
				if(tablecustomer.getNumRows() > 0) {
					String[] customerline = tablecustomer.getString(0).split(";");
					customerid = customerline[0];
					customername = customerline[1];
					customercity = customerline[2];
					customerzip = customerline[3];
					customerregion = customerline[4];
					customercountry = customerline[5];
					customeraddress = customerline[6];
					if(customerline.length > 7)
						customertelephone = customerline[7];
				}
				saleorderpartnerstable.appendRow();
				saleorderpartnerstable.setValue("PARTN_ROLE", "WE");
				System.out.println("PARTN_ROLE: " + "WE");
				saleorderpartnerstable.setValue("PARTN_NUMB", "0000100013");
				System.out.println("PARTN_NUMB: " + "0000100013");
				saleorderpartnerstable.setValue("ITM_NUMBER", "000000");
				System.out.println("ITM_NUMBER: " + "000000");
				saleorderpartnerstable.setValue("NAME", customername);
				System.out.println("NAME: " + customername);
				saleorderpartnerstable.setValue("STREET", customeraddress);
				System.out.println("STREET: " + customeraddress);
				saleorderpartnerstable.setValue("COUNTRY", customercountry);
				System.out.println("COUNTRY: " + customercountry);
				saleorderpartnerstable.setValue("POSTL_CODE", customerzip);
				System.out.println("POSTL_CODE: " + customerzip);
				saleorderpartnerstable.setValue("CITY", customercity);
				System.out.println("CITY: " + customercity);
				saleorderpartnerstable.setValue("TELEPHONE", customertelephone);
				System.out.println("TELEPHONE: " + customertelephone);
				

				if(createorderfunction == null)
		            throw new RuntimeException("BAPI_SALESORDER_CREATEFROMDAT2 not found in SAP.");
		        	System.out.println();
		        try {
		        	createorderfunction.execute(destination, newTransactionId);
		        	System.out.println("return createorder: "+createorderfunction.getTableParameterList().getTable("RETURN"));
		        	
		        } catch(AbapException e) {
		            System.out.println(e.toString());
		            return;
		        } finally {
		        	try {
			        	commitfunction.execute(destination);
			        	commitfunction.execute(destination, newTransactionId);
			        	System.out.println("return commit: "+commitfunction.getExportParameterList().getValue("RETURN"));
			        	
			        } catch(AbapException e) {
			            System.out.println(e.toString());
			            return;
			        }	
		        }
		        
		        
		        //Now to commit the order to be able to view it in sap
		        
		        
		        
			} catch (JCoException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
		}

	public void test_get_bapimonitorlist() throws NoSAPECCException {
		
		try {
				
			PrintStream out = new PrintStream(new FileOutputStream("outputsap.txt"));
			System.setOut(out);
			
			//check if properties file exists if not create one
			loadPropertiesForConnection();
			
			JCoDestination destination = JCoDestinationManager.getDestination("./"+DESTINATION_NAME);
			
			
			JCoFunction function = destination.getRepository().getFunction("BAPI_MONITOR_GETLIST");
			JCoParameterList paramlist = function.getImportParameterList();
			JCoParameterFieldIterator paramfieldit = paramlist.getParameterFieldIterator();
			while(paramfieldit.hasNextField()) {
				JCoField field = paramfieldit.nextField();
				String name = field.getName();
				System.out.println("field.name="+name);
				JCoRecordMetaData meta =  field.getRecordMetaData();
				for(int i=0; i<meta.getFieldCount(); i++) {
					String name2 = meta.getName(i);
					System.out.println("field.meta.name="+name2);
				}
				JCoExtendedFieldMetaData exmeta = field.getExtendedFieldMetaData();
				System.out.println("field.exmeta="+exmeta.toString());
			}
			//JCoTable options = function.getTableParameterList().getTable("CUSTOMER_NUMBER");
			
			//options.appendRow();
			//options.setValue("CUSTOMER", "*");
			
	        if(function == null)
	            throw new RuntimeException("BAPI_MATERIAL_GETLIST not found in SAP.");
	        	System.out.println();
	        try {
	            function.execute(destination);
	        } catch(AbapException e) {
	            System.out.println(e.toString());
	            return;
	        }
	        
	        JCoTable codes = function.getTableParameterList().getTable("BAPILIST");
	        System.out.println("return error: "+function.getExportParameterList().getValue("RETURN"));
	        System.out.println("Num rows: "+codes.getNumRows()+"  Num cols: "+codes.getNumColumns());
	        for(int i = 0; i<codes.getNumRows(); i++) {
	        	codes.setRow(i);
	        	if(codes.getString("ABAPNAME").equals("BAPI_SALESORDER_GETLIST"));
	        	System.out.println("AN: "+codes.getString("ABAPNAME")+" BN: "+codes.getString("BAPINAME")+" T: "+codes.getString("BAPI_TEXT"));
	        	System.out.println("AN: "+codes.getString("ABAPNAME")+" BN: "+codes.getString("BAPINAME")+" T: "+codes.getString("BAPI_TEXT"));
	        }
		} catch (JCoException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	//TODO test function get a list of all materials
	public void test_get_materials() throws NoSAPECCException {
		
		PrintStream out;
		try {
			out = new PrintStream(new FileOutputStream("outputsap.txt"));
			System.setOut(out);
		} catch (FileNotFoundException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		String fields[] = {"MATNR", "LAEDA"};
		JCoTable materials = this.consultSapTable("MARA", ";", "0", "3000", "LAEDA > '20100101'", fields);
		
		materials.setRow(0);
		String materialrow = materials.getString(0);
		String[] splitrow = materialrow.split(";");

		String id = splitrow[0];
		
		//Obtain title of material
		String fields2[] = {"MAKTX"};
		String title = this.consultSapTable("MAKT", "", "0", "1", "MATNR = '"+id+"'", fields2).getString(0);
		
		System.out.println("id: "+id);
		System.out.println("title: "+title);
		
	}
	
	//TODO test function get a list of all functions of table???
	public void test_list_functions_table() throws NoSAPECCException {
		
		
		
	}
	
	static String returnSufixForRefId(String connectiontype) {
		String sufix = "";
		switch (connectiontype) {
			case "mercadolibre":
				sufix = "ML";
				break;
			case "prestashop":
				sufix = "PS";
				break;
			case "woocommerce":
				sufix = "WC";
				break;
			default:
				sufix = "";
				break;
		}
		return sufix;
	}
	
	static double convertToDouble(Object longValue){
		double valueTwo = 0; // whatever to state invalid!
		if(longValue instanceof Long) {
			valueTwo = ((Long) longValue).doubleValue();
		}else if(longValue instanceof Float) {
			valueTwo = ((Float) longValue).doubleValue();
		}else if(longValue instanceof Double) {
			valueTwo = ((Double) longValue).doubleValue();
		}else {
			try {
				valueTwo = (double)valueTwo;
			}catch(Exception e) {
				System.out.println("could not find and cast conversion. longValue is instance of "+longValue.getClass());
			}
		}
		return valueTwo;
	}
	
	public static boolean isValidEmail(String emailStr) {
	        Matcher matcher = VALID_EMAIL_ADDRESS_REGEX .matcher(emailStr);
	        return matcher.find();
	}
	
	@SuppressWarnings("unchecked")
	private JSONObject convertSAPProductToC2SProduct() throws NoSAPECCException {
		
		materials.setRow(currentmaterial);
		String materialrow = materials.getString(0);
		String[] splitrow = materialrow.split(";");
		
		String id = splitrow[0];
		String title;
		
		//Obtain title of material
		String fieldstitle[] = {"MAKTX"};
		String titlesap = "";
		JCoTable titlesaptable = this.consultSapTable("MAKT", "", "0", "1", "MATNR = '"+id+"'", fieldstitle);
		if(titlesaptable.getNumRows() > 0) {
			titlesap = titlesaptable.getString(0);
		}
		
		//Obtain price of material and stock
		String fieldsprice[] = {"STPRS", "LBKUM"};
		String pricesap = "0.0";
		String stocksap = "0";
		JCoTable pricesaptable = this.consultSapTable("MBEW", ";", "0", "1", "MATNR = '"+id+"'", fieldsprice);
		if(pricesaptable.getNumRows() > 0) {
			String[] pricesaptabledata = pricesaptable.getString(0).split(";");
			pricesap = pricesaptabledata[0];
			stocksap = pricesaptabledata[1];
		}
		
		JSONObject product = new JSONObject();
		
		product.put("_id",id);
		product.put("sku",id);
		product.put("title",titlesap);

		
		JSONArray variations = new JSONArray();
		product.put("variations", variations);
		JSONObject variation = new JSONObject();
		variations.add(variation);
		
		JSONArray availabilities = new JSONArray();
		variation.put("availabilities", availabilities);
		
		JSONObject availability = new JSONObject();
		
		availabilities.add(availability);
		availability.put("tag","default");
		availability.put("quantity",Double.parseDouble(stocksap));

		JSONArray prices = new JSONArray();
		variation.put("prices",prices);
		JSONObject price = new JSONObject();
		prices.add(price);
		price.put("tag", "default");
		price.put("currency", delegate.config.getProperty("defaultcurrency"));
		String pricelistoverride = delegate.config.getProperty("pricelistoverride");
		if(pricelistoverride != null && pricelistoverride.length() > 0) {
			
		}
		double priceval = Double.parseDouble(pricesap);
		price.put("number", priceval);
		
		
		String imgurl = "";
		JSONArray images = new JSONArray();
		variation.put("images",images);
		
		JSONArray videos = new JSONArray();
		variation.put("videos", videos);
		
		variation.put("size", "");
		variation.put("color", "");
		
		return product;
		
	}
	
	@SuppressWarnings("unchecked")
	private JSONObject convertSAPOrderToC2SOrder() throws NoSAPECCException {
		
		orders.setRow(currentorder);
		String orderheaderrow = orders.getString(0);
		String[] splitrow = orderheaderrow.split(";");
		
		String orderid = splitrow[0];
		String clientid = splitrow[1];
		String datecreated = splitrow[2];
		
		Double totalamount = 0.0;
		
		//Obtain client details
		String fieldscustomerdetails[] = {"KUNNR", "NAME1", "ORT01", "PSTLZ", "REGIO", "LAND1", "STRAS", "TELF1"};
		String customerid="", customername="", customercity="", customerzip="", customerregion="", customercountry="", customeraddress="", customertelephone="";		
		JCoTable tablecustomer = this.consultSapTable("KNA1", ";", "0", "1", "KUNNR = '"+clientid+"'", fieldscustomerdetails);
		if(tablecustomer.getNumRows() > 0) {
			String[] customerline = tablecustomer.getString(0).split(";");
			customerid = customerline[0];
			customername = customerline[1];
			customercity = customerline[2];
			customerzip = customerline[3];
			customerregion = customerline[4];
			customercountry = customerline[5];
			customeraddress = customerline[6];
			if(customerline.length > 7)
				customertelephone = customerline[7];
		}
		JSONObject buyer = new JSONObject();
		JSONObject billingaddress = new JSONObject();
		JSONObject shipmentaddress = new JSONObject();
		buyer.put("id", customerid);
		buyer.put("phone", customertelephone);
		buyer.put("firstName", customername);
		buyer.put("lastName", customername);
		billingaddress.put("addressline", customeraddress);
		billingaddress.put("city", customercity);
		billingaddress.put("country", customercountry);
		billingaddress.put("state", customerregion);
		billingaddress.put("zipcode", customerzip);
		buyer.put("billingaddress", billingaddress);
		buyer.put("shipmentaddress", billingaddress);
		
		//Obtain order items, quantity and price and put them in orderItems
		String fieldsorderitems[] = {"MATNR", "NETPR", "KWMENG"};
		JCoTable orderitemssaptable = this.consultSapTable("VBAP", ";", "0", "1000", "VBELN = '"+orderid+"'", fieldsorderitems);
		JSONArray orderItems = new JSONArray();
		if(orderitemssaptable.getNumRows() > 0) {
			for(int i=0; i <= orderitemssaptable.getNumRows(); i++) {
				orderitemssaptable.setRow(i);
				String[] split = orderitemssaptable.getString(0).split(";");
				String materialid = split[0];
				String materialprice = split[1].replaceAll("\\s+","");
				materialprice = materialprice.replaceAll("-", "");
				String materialquantity = split[2].replaceAll("\\s+","");
				
				totalamount = totalamount + (Double.parseDouble(materialprice)*Double.parseDouble(materialquantity));
				
				JSONObject orderItem = new JSONObject();
				orderItem.put("id", materialid);
				orderItem.put("variation_id", "");
				orderItem.put("quantity", materialquantity);
				orderItem.put("unitPrice", materialprice);
				orderItem.put("currencyId", delegate.config.getProperty("defaultcurrency"));
				orderItems.add(orderItem);
				
			};
			
		}

		//Obtain order status
		String fieldstatus[] = {"LFSTK"};
		JCoTable statussaptable = this.consultSapTable("VBUK", "", "0", "1000", "VBELN = '"+orderid+"'", fieldstatus);
		String status = "";
		if(statussaptable.getNumRows() > 0) {
			statussaptable.setRow(0);
			status = statussaptable.getString(0);
			
		}
		
		//Obtain shipping address
//		String fieldshippingcode[] = {"ADRNR"};	
//			//Obtain shipping code to get address details
//		JCoTable tableshippingcode = this.consultSapTable("VBPA", "", "0", "1", "VBELN = '"+orderid+"'", fieldshippingcode);
//		String shippingcodeid = tableshippingcode.getString(0);
//			//Obtain shipping address details
//		String fieldshippingaddress[] = {""};
//		JCoTable tableshippingaddress = this.consultSapTable("ADRC", "", "0", "1", "ADDRNUMBER = '"+shippingcodeid+"'", fieldshippingaddress);
		
		
		JSONObject total = new JSONObject();
		total.put("currency", delegate.config.getProperty("defaultcurrency"));
		total.put("amount", totalamount);
		
		JSONObject order = new JSONObject();
		order.put("_id", orderid);
		order.put("orderid", orderid);
		order.put("status", status);
		order.put("dateCreated", datecreated);
		order.put("total", total);
		order.put("buyer", buyer);
		order.put("orderItems", orderItems);
		
		return order;
		
	}
	
	private void loadPropertiesForConnection() throws NoSAPECCException {
		try {
			FileInputStream keyfis = new FileInputStream(new File(DESTINATION_NAME+".jcoDestination"));
			keyfis = null;
		} catch (FileNotFoundException e) {
			ServiceLogger.log("Properties file for conn does not exists, creating one...");
			this.createPropertiesForConnection();
		}
	}
	
	private void createPropertiesForConnection() throws NoSAPECCException {
		try {
			
			Properties connectProperties = new Properties();

			connectProperties.setProperty(DestinationDataProvider.JCO_ASHOST,config.getProperty("ASHOST"));
			connectProperties.setProperty(DestinationDataProvider.JCO_SYSNR,config.getProperty("SYSNR"));
			connectProperties.setProperty(DestinationDataProvider.JCO_CLIENT,config.getProperty("CLIENT"));
			connectProperties.setProperty(DestinationDataProvider.JCO_USER,config.getProperty("USER"));
			connectProperties.setProperty(DestinationDataProvider.JCO_PASSWD,config.getProperty("PASSWD"));
			connectProperties.setProperty(DestinationDataProvider.JCO_LANG,config.getProperty("LANG"));
			connectProperties.setProperty(DestinationDataProvider.JCO_POOL_CAPACITY, config.getProperty("POOLCAPACITY"));
			connectProperties.setProperty(DestinationDataProvider.JCO_PEAK_LIMIT, config.getProperty("PEAKLIMIT"));
			
			createDestinationDataFile("./"+DESTINATION_NAME,connectProperties);

		} catch (Exception e) {
			ServiceLogger.log("Cant create property file: "+ e);
		}
	}
	
	static void createDestinationDataFile(String destinationName,Properties connectProperties) {
	    File destCfg = new File(destinationName+".jcoDestination");
	    try {
	        FileOutputStream fos = new FileOutputStream(destCfg,false);
	        connectProperties.store(fos, "");
	        fos.close();
	    } catch (Exception e) {
	        throw new RuntimeException("Unable to create the destination files", e);
	    }
	 }
	
	private JCoTable consultSapTable(String table, String delimiter, String offset, String rowcount, String options, String[] fields) throws NoSAPECCException{
		try {
			
			JCoTable dataSet = null;
			
			//check if properties file exists if not create one
			loadPropertiesForConnection();
			
			if(destination == null) {
				destination = JCoDestinationManager.getDestination("./"+DESTINATION_NAME);
			}
			
			JCoFunction function = destination.getRepository().getFunction("RFC_READ_TABLE");
			JCoParameterList paramlist = function.getImportParameterList();
			
			paramlist.setValue("QUERY_TABLE", table);
			paramlist.setValue("DELIMITER", ";");
			paramlist.setValue("ROWSKIPS", offset);
			paramlist.setValue("ROWCOUNT", rowcount);
			
			JCoTable optionstable = function.getTableParameterList().getTable("OPTIONS");
			optionstable.appendRow();
			optionstable.setValue("TEXT", options);
			
			JCoTable fieldstable = function.getTableParameterList().getTable("FIELDS");
			for(int i = 0; i<fields.length; i++) {
				fieldstable.appendRow();
				fieldstable.setValue("FIELDNAME", fields[i]);
			}
			
			if(function == null)
				throw new RuntimeException("RFC_READ_TABLE not found in SAP.");
			
			try {
				function.execute(destination);
			} catch (JCoException e) {
				throw new NoSAPECCException("SAP ECC error executing function..." + e);
			}
			
			JCoTable codes = function.getTableParameterList().getTable("DATA");
			dataSet = codes;
			
//			System.out.println("Num rows: "+codes.getNumRows()+"  Num cols: "+codes.getNumColumns());
//			for(int i = 0; i<codes.getNumRows(); i++){
//				codes.setRow(i);
//				System.out.println(codes.getString(0));
//			}
			
			
			return dataSet;
		} catch (JCoException e) {
			// TODO Auto-generated catch block
			throw new NoSAPECCException("SAP ECC error consulting table..." + e);
		}
	}

}
