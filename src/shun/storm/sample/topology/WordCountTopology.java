package shun.storm.sample.topology;

import shun.storm.sample.bolt.*;
import shun.storm.sample.spout.*;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class WordCountTopology {

	public static void main(String[] args) throws Exception {

		// arguments check.
		if (args.length == 0) {
			System.err.println("[error] 1 argument (Topology name) required.");
			System.exit(-1);
		}

		// creating an instance of TopologyBuilder.
		TopologyBuilder builder = new TopologyBuilder();
		
		// registering the RandomSentenceSpout and assigning it a unique ID.
		builder.setSpout("spout", new RandomSentenceSpout(), 1);
		// RandomSentenceSpout --> SplitEnglishSentenceBolt
		builder.setBolt("split", new SplitEnglishSentenceBolt(), 4).shuffleGrouping("spout");
		// SplitEnglishSentenceBolt --> WordCountBolt
		builder.setBolt("count", new WordCountBolt(), 5)
				.fieldsGrouping("split", new Fields("word"));

		Config conf = new Config();
		conf.setDebug(false);
		conf.setNumWorkers(10);

		StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
	}

}
