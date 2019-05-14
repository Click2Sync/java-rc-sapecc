package com.click2sync.rc.srv.sapecc;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.Properties;

import org.json.simple.JSONObject;

public class Main {
	
	final String configFile = "./config.properties";
	Properties config;
	SAPECCProxy sap;
	C2SProxy c2s;
	Long longpauseMillis = 10000L;
	Long normalpauseMillis = 1000L;
	static Main app;
	
	Main(){
		
		c2s = new C2SProxy(this);
		sap = new SAPECCProxy(this);
		config = new Properties();
		InputStream input;
		try {
			input = new FileInputStream(configFile);
			config.load(input);
			sap.setProperties(config);
			longpauseMillis = Long.parseLong(config.getProperty("longpausemillis"));
			normalpauseMillis = Long.parseLong(config.getProperty("normalpausemillis"));
		} catch (NumberFormatException e) {
			ServiceLogger.error(new C2SRCServiceException("Could not read pause time settings from configuration: "+e.getMessage()));
		} catch (FileNotFoundException e) {
			ServiceLogger.error(new C2SRCServiceException("Configuration file not found: "+e.getMessage()));
		} catch (IOException e) {
			ServiceLogger.error(new C2SRCServiceException("Could not access configuration file: "+e.getMessage()));
		}
		
	}
	
	public void loop() throws InterruptedException {
		
		try {
			work();
			pauseWork(true);
		} catch (C2SUnreachableException e) {
			ServiceLogger.error(e);
			pauseWork(true);
		} catch (NoSAPECCException e) {
			ServiceLogger.error(e);
			pauseWork(true);
		} catch (C2SRCServiceException e) {
			ServiceLogger.error(e);
			pauseWork(true);
		}
		
	}
	
	public void pauseWork(boolean longPause) throws InterruptedException {
		
		if(longPause) {
			Thread.sleep(longpauseMillis);
		} else {
			Thread.sleep(normalpauseMillis);
		}
		
	}
	
	public void senseEnvironmentForSAPServices() throws NoSAPECCException {
		
		sap.sense();
		
	}
	
	public void senseEnvironmentForC2SReachability() throws C2SUnreachableException {
		
		c2s.sense();
		
	}
	
	public static void abstractLoop() {
		
		ServiceLogger.log("loop");
		if(app == null) {
			app = new Main();
			Runtime.getRuntime().addShutdownHook(new Thread(new ShutdownHook()));
			try {
				app.senseEnvironmentForSAPServices();
				app.senseEnvironmentForC2SReachability();
			} catch (NoSAPECCException e) {
				ServiceLogger.error(e);
				ServiceLogger.error(e);
				try {
					app.pauseWork(true);
					app = null;
				} catch (InterruptedException e1) {
					ServiceLogger.error(e1);
				}
				return;
			} catch (C2SUnreachableException e) {
				ServiceLogger.error(e);
				try {
					app.pauseWork(true);
					app = null;
				} catch (InterruptedException e1) {
					ServiceLogger.error(e1);
				}
				return;
			}
		}
		try {
			app.loop();
		} catch (InterruptedException e) {
			ServiceLogger.error(e);
		}
		
	}

	public static void main(String[] args) {
		
		if(args.length > 0) {//normal service run
			boolean cli = false;
			boolean test_read_props_file = false;
			boolean test_get_orders = false;
			boolean test_get_materials = false;
			boolean test_list_functions_table = false;
			for(int i=0; i<args.length; i+=1) {
				System.out.println(args[i]);
				if(args[i].equals("--cli")) {
					cli = true;
				}else if(args[i].equals("--test_read_props_file")) {
					test_read_props_file = true;
				}else if(args[i].equals("--test_get_orders")) {
					test_get_orders = true;
				}else if(args[i].equals("--test_get_materials")) {
					test_get_materials = true;
				}else if(args[i].equals("--test_list_functions_table")) {
					test_list_functions_table = true;
				}
			}
			if(cli) {
				System.out.println("cli true");
				if(test_read_props_file) {
					app = new Main();
					app.test_read_props_file();
				}else if(test_get_orders) {
					app = new Main();
					app.test_get_orders();
				}else if(test_get_materials) {
					app = new Main();
					app.test_get_materials();
				}else if(test_list_functions_table) {
					app = new Main();
					app.test_list_functions_table();
				}else {
					System.out.println("Could not understand the intended operation");
				}
			}else {
				System.out.println("Could not understand the intended operation");
			}
			
		}else {//normal service run
			while(true) {
				try {
					abstractLoop();
				}catch(Exception e) {
					ServiceLogger.error(e);
				}
			}
		}

	}
	
	@SuppressWarnings("unused")
	private void test_read_props_file() {
		
		try {
			sap.test_read_props_file();
		} catch (NoSAPECCException e) {
			e.printStackTrace();
		}
		
	}
	
	@SuppressWarnings("unused")
	private void test_get_orders() {
		
		try {
			sap.test_get_orders();
		} catch (NoSAPECCException e) {
			e.printStackTrace();
		}
		
	}
	
	@SuppressWarnings("unused")
	private void test_get_materials() {
		
		try {
			sap.test_get_materials();
		} catch (NoSAPECCException e) {
			e.printStackTrace();
		}
		
	}
	
	@SuppressWarnings("unused")
	private void test_list_functions_table() {
		
		try {
			sap.test_list_functions_table();
		} catch (NoSAPECCException e) {
			e.printStackTrace();
		}
		
	}
	
	private boolean checkIfWeAreFetchingRemote() throws C2SUnreachableException, C2SRCServiceException {
		
		return c2s.checkIfFetchingRemote();
	}
	
	private boolean checkIfWeArePullingToRemote() throws C2SUnreachableException, C2SRCServiceException {
		
		return c2s.checkIfPullingToRemote();
	}
	
	private void fetchRemote() throws C2SUnreachableException,NoSAPECCException, InterruptedException {
		
		boolean uploadProducts = true;
		boolean uploadOrders = true;
		ServiceLogger.log("Fetching remote...");
		String strategy = c2s.getStrategy();
		String upstreamstatus = c2s.getUpstreamStatus();
		String entity = c2s.getEntity();
		Long offsetdate = c2s.getCursorOffset();
		
		if(upstreamstatus != null && upstreamstatus.equals("waiting")) {
			//if is waiting, good, change to initialized
			c2s.setInitializeUpload(strategy);
			upstreamstatus = c2s.getUpstreamStatus();
			//(if webonline responds error, then notify of strange behavior to server)
		}
		if(upstreamstatus != null && upstreamstatus.equals("initialized")) {
			//if is initialized, check cursor
			//from cursor start upload
			//when packet uploaded, renew offsets
			if(entity != null && entity.equals("products")) {
				if(uploadProducts) {
					if(strategy != null && strategy.equals("pingsample")) {
						sap.setProductsStreamCursor(offsetdate);
						if(sap.hasMoreProducts(offsetdate)) {
							JSONObject product = sap.nextProduct();
							c2s.setProductToUploadOnBuffer(product, strategy);
						}
					} else {
						ServiceLogger.log("Get products not pingsample");
						sap.setProductsStreamCursor(offsetdate);
						while(sap.hasMoreProducts(offsetdate)) {
							JSONObject product = sap.nextProduct();
							c2s.setProductToUploadOnBuffer(product, strategy);
						}
					}
				}
				c2s.setFinishProductUpload(strategy);
				offsetdate = c2s.getCursorOffset();
				entity = c2s.getEntity();
			}
			if(entity != null && entity.equals("orders")) {
				if(uploadOrders) {
					if(strategy != null && strategy.equals("pingsample")) {
						sap.setOrdersStreamCursor(offsetdate);
						if(sap.hasMoreOrders(offsetdate)) {
							JSONObject order = sap.nextOrder();
							c2s.setOrderToUploadOnBuffer(order, strategy);
						}
					} else {
						sap.setOrdersStreamCursor(offsetdate);
						while(sap.hasMoreOrders(offsetdate)) {
							JSONObject order = sap.nextOrder();
							c2s.setOrderToUploadOnBuffer(order, strategy);
						}
					}
				}
				c2s.setFinishOrderUpload(strategy);
				offsetdate = c2s.getCursorOffset();
			}
			//when no more in SAP, send finish command
			c2s.setFinishUpload(strategy);
			upstreamstatus = c2s.getUpstreamStatus();
		}
		if(upstreamstatus != null && (upstreamstatus.equals("finished") || upstreamstatus.equals("finishing"))) {
			//if is is finished, wait until server finishes
			ServiceLogger.log("Upload status is finished... waiting for C2S to complete the overall process...");
		}else {
			//unknown upstreamstatus code it means we crashed, notify of strange behavior to server... report?
			ServiceLogger.error("Unknown upstreamstatus code="+upstreamstatus+". Corrupt connection metadata...");
		}
		
		pauseWork(true);
		
	}
	
	private void pullToRemote() throws C2SUnreachableException,NoSAPECCException, InterruptedException {

		boolean downloadProducts = true;
		boolean downloadOrders = true;
		ServiceLogger.log("Pulling from remote...");
		String upstreamstatus = c2s.getUpstreamStatus();
		String entity = c2s.getEntity();
		
		if(upstreamstatus.equals("waiting")) {
			//if is waiting, good, change to initialized
			c2s.setInitializeDownload();
			upstreamstatus = c2s.getUpstreamStatus();
			//(if webonline responds error, then notify of strange behavior to server)
		}
		if(upstreamstatus.equals("initialized")) {
			//if is initialized, check cursor
			//from cursor start upload
			//when packet uploaded, renew offsets
			if(entity.equals("products")) {
				if(downloadProducts) {
					while(c2s.hasMoreUpdatedProducts(0)) {
						JSONObject product = c2s.nextProduct();
						if(product != null) {
							String id = (String) product.get("sku");
							try {
								JSONObject productstored = sap.storeProduct(product);
								c2s.sendProductPullSuccessNotification(id, productstored, true, "");
							}catch(NoSAPECCException e) {
								c2s.sendProductPullSuccessNotification(id, null, false, e.getMessage());
							}							
						}
					}
				}
				c2s.setFinishProductDownload();
				entity = c2s.getEntity();
			}
			if(entity.equals("orders")) {
				if(downloadOrders) {
					while(c2s.hasMoreUpdatedOrders(0)) {
						JSONObject order = c2s.nextOrder();
						String id = (String) order.get("orderid");
						try {
							JSONObject orderstored = sap.storeOrder(order);
							c2s.sendOrderPullSuccessNotification(id, orderstored, true, "");
						}catch(NoSAPECCException e) {
							ServiceLogger.error(e);
							c2s.sendOrderPullSuccessNotification(id, null, false, e.getMessage());
						}
					}
				}
				c2s.setFinishOrderDownload();
			}
			//when no more in SAP, send finish command
			c2s.setFinishDownload();
			upstreamstatus = c2s.getUpstreamStatus();
		}
		if(upstreamstatus.equals("finished") || upstreamstatus.equals("finishing")) {
			//if is is finished, wait until server finishes
			ServiceLogger.log("Download status is finished... waiting for C2S to complete the overall process...");
		}else {
			//unknown upstreamstatus code it means we crashed, notify of strange behavior to server... report?
			ServiceLogger.error("Unknown upstreamstatus code="+upstreamstatus+". Corrupt connection metadata...");
		}
		
		pauseWork(true);
		
	}
	
	private void work() throws C2SUnreachableException,NoSAPECCException,C2SRCServiceException, InterruptedException {
		
		if(checkIfWeAreFetchingRemote()) {
			fetchRemote();
		}
		if(checkIfWeArePullingToRemote()) {
			pullToRemote();
		}
		
	}
	
	private static class ShutdownHook implements Runnable {
		
		public void run() {
			onStop();
		}
		
		private void onStop() {
			ServiceLogger.error("Ended at " + new Date());
			System.out.flush();
			System.out.close();
		}
		
	}

}
