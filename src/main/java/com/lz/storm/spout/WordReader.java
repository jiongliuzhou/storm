package com.lz.storm.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Map;

/**
 * 读取字符
 * @author DELL
 * @create 2020/1/2
 * @since 1.0.0
 */
public class WordReader implements IRichSpout{
    private static final long serialVersionUID = 1L;
    private SpoutOutputCollector collector;
    private FileReader fileReader;

    @Override
    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        try {
            this.fileReader=new FileReader(map.get("wordsFile").toString());
        }catch (Exception e){
            e.printStackTrace();
        }
        this.collector=spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        String str;
        BufferedReader reader = new BufferedReader(fileReader);
        try {
            while ((str = reader.readLine()) != null) {
                //发射每一行，Values是一个ArrayList的实现
                this.collector.emit(new Values(str), str);
            }
        } catch (Exception e) {
            throw new RuntimeException("Error reading tuple", e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line"));
    }

    @Override
    public void close() {

    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }

    @Override
    public void ack(Object o) {

    }

    @Override
    public void fail(Object o) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
