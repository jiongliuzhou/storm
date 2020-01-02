package com.lz.storm.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * @author DELL
 * @create 2020/1/2
 * @since 1.0.0
 */
public class WordCounter implements IRichBolt{
    Integer id;
    String name;
    private Map<String, Integer> counters;
    private OutputCollector collector;

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.counters = new HashMap<>();
        this.collector = outputCollector;
        this.name = topologyContext.getThisComponentId();
        this.id = topologyContext.getThisTaskId();
    }

    @Override
    public void execute(Tuple tuple) {
        String str = tuple.getString(0);
        if (!counters.containsKey(str)) {
            counters.put(str, 1);
        } else {
            Integer c = counters.get(str) + 1;
            counters.put(str, c);
        }
        // 确认成功处理一个tuple
        collector.ack(tuple);
    }

    @Override
    public void cleanup() {
        for(Map.Entry<String, Integer> s:counters.entrySet()){
            System.out.println(s.getKey()+" count:"+s.getValue());
        }
        counters.clear();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
