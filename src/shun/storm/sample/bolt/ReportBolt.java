package shun.storm.sample.bolt;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class ReportBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;

	// "top10Map" store top 10 hashtags and their corresponding counts.
	Map<String, Integer> top10Map = new HashMap<String, Integer>();
	String minWord;
	int minCount;

	OutputCollector _collector;

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context,
			OutputCollector collector) {
		// saves a reference to the OutputCollector object.
		_collector = collector;
	}

	@Override
	public void execute(Tuple tuple) {
		// look up the count for the word received.
		String word = tuple.getStringByField("word");
		int count = tuple.getIntegerByField("count");

		if (top10Map.size() < 10 || count > minCount) {
			// put the map
			top10Map.put(word, count);
			// remove min word
			if (top10Map.size() > 10) {
				top10Map.remove(minWord);
			}
			// update min
			minCount = 0;
			for (Map.Entry<String, Integer> e : top10Map.entrySet()) {
				if (minCount == 0 || minCount > e.getValue()) {
					minCount = e.getValue();
					minWord = e.getKey();
				}
			}
			// print
			printTop10();
		}

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	void printTop10() {
		// create sorted list.
		List<Map.Entry<String, Integer>> entries = new ArrayList<Map.Entry<String, Integer>>(
				top10Map.entrySet());
		Collections.sort(entries, new Comparator<Map.Entry<String, Integer>>() {
			@Override
			public int compare(Entry<String, Integer> entry1, Entry<String, Integer> entry2) {
				return ((Integer) entry2.getValue()).compareTo((Integer) entry1.getValue());
			}
		});

		// print top 10 hashtags.
		System.out.println("===== Top 10 Hashtags =====");
		System.out.println("#	 : Word");
		System.out.println("----------------------");
		for (Entry<String, Integer> e : entries) {
			System.out.println(e.getValue() + "		 : " + e.getKey());
		}
		System.out.println("===========================");
		System.out.println(" ");
	}

}
