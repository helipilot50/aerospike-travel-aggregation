package com.aerospike.examples.travel;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.log4j.Logger;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Language;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.ResultSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.IndexTask;
import com.aerospike.client.task.RegisterTask;


/**
@author Peter Milne
*/
public class FlighAggregation {
	private static final String ASSOC = "assoc";
	private static final String PAX = "pax";
	private static final String ORIGDEST = "origdest";
	private static final String DEST = "dest";
	private static final String ORIG = "orig";
	private AerospikeClient client;
	private String seedHost;
	private int port;
	private String namespace;
	private String set;
	private WritePolicy writePolicy;
	private Policy policy;

	private static Logger log = Logger.getLogger(FlighAggregation.class);
	public FlighAggregation(String host, int port, String namespace, String set) throws AerospikeException {
		this.client = new AerospikeClient(host, port);
		this.seedHost = host;
		this.port = port;
		this.namespace = namespace;
		this.set = set;
		this.writePolicy = new WritePolicy();
		this.policy = new Policy();
	}
	public FlighAggregation(AerospikeClient client, String namespace, String set) throws AerospikeException {
		this.client = client;
		this.namespace = namespace;
		this.set = set;
		this.writePolicy = new WritePolicy();
		this.policy = new Policy();
	}
	public static void main(String[] args) throws AerospikeException {
		try {
			Options options = new Options();
			options.addOption("h", "host", true, "Server hostname (default: 127.0.0.1)");
			options.addOption("p", "port", true, "Server port (default: 3000)");
			options.addOption("n", "namespace", true, "Namespace (default: test)");
			options.addOption("s", "set", true, "Set (default: demo)");
			options.addOption("u", "usage", false, "Print usage.");

			CommandLineParser parser = new PosixParser();
			CommandLine cl = parser.parse(options, args, false);


			String host = cl.getOptionValue("h", "127.0.0.1");
			String portString = cl.getOptionValue("p", "3000");
			int port = Integer.parseInt(portString);
			String namespace = cl.getOptionValue("n", "test");
			String set = cl.getOptionValue("s", "demo");
			log.debug("Host: " + host);
			log.debug("Port: " + port);
			log.debug("Namespace: " + namespace);
			log.debug("Set: " + set);

			@SuppressWarnings("unchecked")
			List<String> cmds = cl.getArgList();
			if (cmds.size() == 0 && cl.hasOption("u")) {
				logUsage(options);
				return;
			}

			FlighAggregation as = new FlighAggregation(host, port, namespace, set);

			as.init();
			as.work();

		} catch (Exception e) {
			log.error("Critical error", e);
		}
	}
	public void work() {
		/*
		 * create statement
		 */
		Statement stmt = new Statement();
		stmt.setNamespace(this.namespace);
		stmt.setSetName(this.set);
		stmt.setBinNames(ORIG, DEST, ORIGDEST, PAX, ASSOC);
		stmt.setFilters(Filter.equal(ORIG, "LAX"));
		/*
		 * execute query with Aggregation
		 */
		ResultSet results = this.client.queryAggregate(null, stmt, "flights", "stats");
		/*
		 * process results.
		 * 
		 * Should be:
		 * ResultSet with 3 records, count of {Origin,Dest,Pax}
		 * 				org,dest,pax,count
		 * 				LAX,DFW,1,3
		 * 				LAX,DFW,2,2
		 * 				LAX,DFW,3,1
		 */
		try{
			while(results.next()){
				log.info(results.getObject().toString());
			}
		} finally {
			results.close();
		}
		
	}
	/**
	 * Write usage to console.
	 */
	private static void logUsage(Options options) {
		HelpFormatter formatter = new HelpFormatter();
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		String syntax = FlighAggregation.class.getName() + " [<options>]";
		formatter.printHelp(pw, 100, syntax, "options:", options, 0, 2, null);
		log.info(sw.toString());
	}

	public void init() throws Exception {
		/*
		 * create some flight records
		 * 
		 * 3501892936,92,12:00.0,LAX,DFW,LAXDFW,1,Google
 		 * 3501892964,92,13:00.0,LAX,DFW,LAXDFW,1,Google
 		 * 3501892970,92,13:00.0,LAX,DFW,LAXDFW,3,Google
 		 * 3501893361,92,34:00.0,LAX,DFW,LAXDFW,2,Google
 		 * 3501896150,92,09:00.0,LAX,DFW,LAXDFW,1,MSN
 		 * 3501896170,92,10:00.0,LAX,DFW,LAXDFW,2,MSN
 		 * 
		 */
		this.client.put(null, new Key(this.namespace, this.set, "3501892936,92,12:00.0"), 
				new Bin(ORIG, "LAX"), 
				new Bin(DEST,"DFW"),
				new Bin(ORIGDEST, "LAXDFW"),
				new Bin(PAX, 1),
				new Bin(ASSOC, "Google"));
		this.client.put(null, new Key(this.namespace, this.set, "3501892964,92,13:00.0"), 
				new Bin(ORIG, "LAX"), 
				new Bin(DEST,"DFW"),
				new Bin(ORIGDEST, "LAXDFW"),
				new Bin(PAX, 1),
				new Bin(ASSOC, "Google"));
		this.client.put(null, new Key(this.namespace, this.set, "3501892970,92,13:00.0"), 
				new Bin(ORIG, "LAX"), 
				new Bin(DEST,"DFW"),
				new Bin(ORIGDEST, "LAXDFW"),
				new Bin(PAX, 3),
				new Bin(ASSOC, "Google"));
		this.client.put(null, new Key(this.namespace, this.set, "3501893361,92,34:00.0"), 
				new Bin(ORIG, "LAX"), 
				new Bin(DEST,"DFW"),
				new Bin(ORIGDEST, "LAXDFW"),
				new Bin(PAX, 2),
				new Bin(ASSOC, "Google"));
		this.client.put(null, new Key(this.namespace, this.set, "3501896150,92,09:00.0"), 
				new Bin(ORIG, "LAX"), 
				new Bin(DEST,"DFW"),
				new Bin(ORIGDEST, "LAXDFW"),
				new Bin(PAX, 1),
				new Bin(ASSOC, "MSN"));
		this.client.put(null, new Key(this.namespace, this.set, "3501896170,92,10:00.0"), 
				new Bin(ORIG, "LAX"), 
				new Bin(DEST,"DFW"),
				new Bin(ORIGDEST, "LAXDFW"),
				new Bin(PAX, 2),
				new Bin(ASSOC, "MSN"));
		
		/*
		 * create an index orig
		 */
		IndexTask indexTask = this.client.createIndex(null, this.namespace, this.set, "origin-index", ORIG, IndexType.STRING);
		indexTask.waitTillComplete();
		/*
		 * register Stream UDF
		 */
		RegisterTask registerTask = this.client.register(null, "udf/flights.lua", "flights.lua", Language.LUA);
		registerTask.waitTillComplete();
	}

}