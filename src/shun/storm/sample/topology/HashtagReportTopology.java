package shun.storm.sample.topology;

import shun.storm.sample.bolt.*;
import shun.storm.sample.spout.*;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class HashtagReportTopology {

	// Twitter OAuth key
	final static String CONSUMER_KEY = "";
	final static String CONSUMER_SECRET = "";
	final static String ACCESS_TOKEN = "";
	final static String ACCESS_TOKEN_SECRET = "";

	public static void main(String[] args) throws Exception {

		// arguments check.
		if (args.length == 0) {
			System.err.println("[error] 1 argument (Topology name) required.");
			System.exit(-1);
		}

		// creating an instance of TopologyBuilder.
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("spout", new TwitterSampleSpout(CONSUMER_KEY, CONSUMER_SECRET,
				ACCESS_TOKEN, ACCESS_TOKEN_SECRET), 1);
		builder.setBolt("extract-text", new ExtractTweetTextBolt(), 4).shuffleGrouping("spout");
		builder.setBolt("extract-tag", new ExtractHashtagBolt(), 4).shuffleGrouping("extract-text");
		builder.setBolt("count", new WordCountBolt(), 5)
				.fieldsGrouping("extract-tag", new Fields("word"));
		builder.setBolt("report", new ReportBolt(), 1).shuffleGrouping("count");

		Config conf = new Config();
		conf.setDebug(false);
		conf.setNumWorkers(10);

		StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
	}

}
