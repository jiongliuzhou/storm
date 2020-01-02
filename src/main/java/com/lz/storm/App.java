package com.lz.storm;

import com.lz.storm.bolt.WordCounter;
import com.lz.storm.bolt.WordNormalizer;
import com.lz.storm.spout.WordReader;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

/**
 * main程序
 * @author DELL
 * @create 2020/1/2
 * @since 1.0.0
 */
public class App {
    public static void main(String[] args) throws Exception{
        //定义一个Topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word-reader",new WordReader(),1);
        builder.setBolt("word-normalizer", new WordNormalizer()).shuffleGrouping("word-reader");
        builder.setBolt("word-counter", new WordCounter(),2).fieldsGrouping("word-normalizer", new Fields("word"));
        //配置
        Config conf = new Config();
        conf.put("wordsFile", "E:\\storm\\word.txt");
        conf.setDebug(false);
        //提交Topology
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        //创建一个本地模式cluster
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Getting-Started-Toplogie", conf,builder.createTopology());
        Utils.sleep(3000);
        cluster.killTopology("Getting-Started-Toplogie");
        cluster.shutdown();
    }
}
