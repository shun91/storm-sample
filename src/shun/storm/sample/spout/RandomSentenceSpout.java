package shun.storm.sample.spout;

import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class RandomSentenceSpout extends BaseRichSpout {
	private static final long serialVersionUID = 1L;

	SpoutOutputCollector _collector;
	Random _rand;

	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		// stores a reference to the SpoutOutputCollector object in an instance variable.
		_collector = collector;
		_rand = new Random();
	}

	@Override
	public void nextTuple() {
		Utils.sleep(100);
		String[] sentences = new String[] { "the cow jumped over the moon",
				"an apple a day keeps the doctor away", "four score and seven years ago",
				"snow white and the seven dwarfs", "i am at two with nature" };
		String sentence = sentences[_rand.nextInt(sentences.length)];
		// emit the sentence.
		_collector.emit(new Values(sentence));
		System.out.println("[RandomSentenceSpout] " + sentence);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// declaring that our spout will emit a single (default) stream of tuples containing a
		// single field ("word").
		declarer.declare(new Fields("word"));
	}

}