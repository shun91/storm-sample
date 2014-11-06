package shun.storm.sample.spout;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;
import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class TwitterSampleSpout extends BaseRichSpout {

	// select tweet language
	boolean isLang = false;

	// Tweet language
	final String lang = "en";

	// Twitter OAuth key
	String consumerKey;
	String consumerSecret;
	String accessToken;
	String accessTokenSecret;

	private static final long serialVersionUID = 1L;

	SpoutOutputCollector _collector;
	LinkedBlockingQueue<Status> queue = null;
	TwitterStream _twitterStream;

	boolean debug = true;

	public TwitterSampleSpout(String consumerKey, String consumerSecret, String accessToken,
			String accessTokenSecret) {
		this.consumerKey = consumerKey;
		this.consumerSecret = consumerSecret;
		this.accessToken = accessToken;
		this.accessTokenSecret = accessTokenSecret;
	}

	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		queue = new LinkedBlockingQueue<Status>(1000);
		_collector = collector;

		// twitterのoauth認証
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(debug).setOAuthConsumerKey(consumerKey)
				.setOAuthConsumerSecret(consumerSecret).setOAuthAccessToken(accessToken)
				.setOAuthAccessTokenSecret(accessTokenSecret);

		_twitterStream = new TwitterStreamFactory(cb.build()).getInstance();

		StatusListener listener = new StatusListener() {

			@Override
			public void onStatus(Status status) {
				queue.offer(status);
			}

			@Override
			public void onDeletionNotice(StatusDeletionNotice sdn) {
			}

			@Override
			public void onTrackLimitationNotice(int i) {
			}

			@Override
			public void onScrubGeo(long l, long l1) {
			}

			@Override
			public void onException(Exception e) {
			}

			@Override
			public void onStallWarning(StallWarning arg0) {
			}

		};
		_twitterStream.addListener(listener);
		_twitterStream.sample();
	}

	@Override
	public void nextTuple() {
		Status ret = queue.poll();
		if (ret != null) {
			if (isLang && !lang.equals(ret.getLang())) {
				return;
			}
			_collector.emit(new Values(ret));
			System.out.println("[TwitterSampleSpout] " + ret);
		}
	}

	@Override
	public void close() {
		_twitterStream.shutdown();
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config ret = new Config();
		ret.setMaxTaskParallelism(1);
		return ret;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet"));
	}

}
