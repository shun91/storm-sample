package shun.storm.sample.bolt;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class WordCountBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
	
	// "counts" store all the words and their corresponding counts.
	Map<String, Integer> counts = new HashMap<String, Integer>();

	OutputCollector _collector;

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context,
			OutputCollector collector) {
		// saves a reference to the OutputCollector object.
		_collector = collector;
	}

	@Override
	public void execute(Tuple tuple) {
		// look up the count for the word received (initializing it to 0 if necessary),
		String word = tuple.getStringByField("word");
		int count=0;
		if(counts.containsKey(word)){
			count = counts.get(word);
		}
		// increment the count.
		count++;
		// store the count.
		counts.put(word, count);
		System.out.println(word+": "+count);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
