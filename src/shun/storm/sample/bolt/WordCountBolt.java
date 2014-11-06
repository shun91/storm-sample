package shun.storm.sample.bolt;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class WordCountBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
	
	Map<String, Integer> counts = new HashMap<String, Integer>();

	OutputCollector _collector;

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context,
			OutputCollector collector) {
		_collector = collector;
	}

	@Override
	public void execute(Tuple tuple) {
		String word = tuple.getString(0);
		int count=0;
		if(counts.containsKey(word)){
			count = counts.get(word);
		}
		count++;
		counts.put(word, count);
		System.out.println(word+": "+count);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word", "count"));
	}

}
