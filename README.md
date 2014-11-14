storm-sample
======================
Apache Stormで実行できるプログラムの簡単なコードです．  
以下の2つのTopologyを実装しています．

+WordCount
+HashtagReport

各Topologyの説明
------

###WordCount

ランダムに生成される文章中に出現する単語の出現回数を単語毎にカウントします．

###HashtagReport

Twitter Streaming APIを利用してTweetを取得し，その中に含まれるハッシュタグの出現回数をタグごとにカウントします．
そして，出現回数の多いハッシュタグ上位10件を出力します．	

実行するまでの流れ
------

1. ソースコード一式をDLし，eclipseにインポートする．
2. 下記3つのライブラリをDLし，ビルドパスに追加する．（これらは全てstormのlibディレクトリにあります）
	* storm-core-0.9.2-incubating.jar
	* twitter4j-core-4.0.1.jar
	* twitter4j-stream-4.0.1.jar
3. jarファイルを作成する（エクスポート）．
4. 作成したjarをStormクラスタにコピー．
5. jarを置いてあるディレクトリに移動して，下記コマンドで実行．

	# WordCount
    storm jar storm-sample.jar shun.storm.sample.topology.WordCountTopology TOPOLOGY_NAME
	# HashtagReport
	storm jar storm-sample.jar shun.storm.sample.topology.HashtagReportTopology TOPOLOGY_NAME

【注意】
HashtagReportTopologyを実行するには，Twitter APIにアクセスするためのkeyを取得する必要があります．（方法はググれば出てきます）
取得したkeyを「HashtagReportTopology.java」の13~16行目に追加してください．