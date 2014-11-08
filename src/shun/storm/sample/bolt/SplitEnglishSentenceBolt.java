package shun.storm.sample.bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SplitEnglishSentenceBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;

	OutputCollector _collector;

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context,
			OutputCollector collector) {
		// saves a reference to the OutputCollector object.
		_collector = collector;
	}

	@Override
	public void execute(Tuple tuple) {
		// looks up the value of the "word" field of the incoming tuple as a string.
		String sentence = tuple.getStringByField("word");
		// splits the value into individual words.
		String[] wordArray = sentence.split("\\s", 0);
		// emits a new tuple for each word.
		for (String word : wordArray) {
			_collector.emit(new Values(word));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// declares a single stream of tuples, each containing one field ("word").
		declarer.declare(new Fields("word"));
	}

}
