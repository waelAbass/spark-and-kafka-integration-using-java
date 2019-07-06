/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.wordcountconsumer;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

/**
 *
 * @author AFAQE3
 */
public class datakafka {

    public static void main(String[] args) throws InterruptedException {
        // local[*] means use all cores of processor in local mode
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("using kafka ");
        // set  batch interval each 5 seconds 
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));
        // kafka utils
        final Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaParams.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaParams.put("group.id", "DEFAULT_GROUP_ID");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
        // kafka topic name "testkafak" and if we use more than one topic we write it as 
        // as "testkafak,mysecondtopic"
        Collection<String> topics = Arrays.asList("second");
        // consume data from kafka 
        JavaInputDStream<ConsumerRecord<String, String>> stream
                = KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );
        // flat map each message 
        JavaDStream<String> wordsd = stream.flatMap(new FlatMapFunction<ConsumerRecord<String, String>, String>() {
            @Override
            public Iterator<String> call(ConsumerRecord<String, String> t) throws Exception {
                //    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
                return Arrays.asList(t.value().split(" ")).iterator();
            }
        });
        // make each element in the form of  tuple2 <key, 1>
        JavaPairDStream<String, Integer> pairs = wordsd.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) {
                        return new Tuple2<>(s, 1);
                    }
                });
        //  reduce by key to count values for each pair
        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer t1, Integer t2) throws Exception {
                // throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
                return t1 + t2;
            }
        });
        
        
          wordCounts.print();
        // spark as producer to kafka
        wordCounts.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {

            @Override
            public void call(JavaPairRDD<String, Integer> t) throws Exception {
                // throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
                List<Tuple2<String, Integer>> d = t.collect();
                KafkaProducer kp = new KafkaProducer(kafkaParams);
                for (Tuple2<String, Integer> x : d) {
                    ProducerRecord pr = new ProducerRecord("testbig", "this message come from producer: " + x._1() + " and count " + x._2);
                    kp.send(pr);
//System.out.println("here"+pr.value());
                }
            }
        });
      //  wordCounts.print();
        jssc.start();
        jssc.awaitTermination();
    }
}
