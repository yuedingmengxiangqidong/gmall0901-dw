package com.atguigugmall0901.dw.logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall0901.dw.common.constant.GmallConstant;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

@RestController      //=Controller + Responsebody
public class LoggerController {

    //自动注入
    @Autowired
    KafkaTemplate kafkaTemplate;

    //要和类名保持一致
    private static final  org.slf4j.Logger logger = LoggerFactory.getLogger(LoggerController.class) ;

    @PostMapping("log")
    @ResponseBody
    public String log(@RequestParam ("log") String logJson) {
        System.out.println(logJson);
        //通过json变成结构化的内存对象
        JSONObject jsonObject = JSON.parseObject(logJson);
        //存放时间撮    为当前时间的毫秒数
        jsonObject.put("ts",System.currentTimeMillis());
        //添加完后在变回去

        sendKafka(jsonObject);//实时数据

        logger.info(jsonObject.toString());//离线的
        //在web里面会认为是一个页面
        return "success";
    }

    private void sendKafka(JSONObject jsonObject) {
        //根据logJson中type   来分到不同的topic中
        if(jsonObject.getString("type").equals("startup")) {
            kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_STARTUP, jsonObject.toJSONString());
        } else {
            kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_EVENT, jsonObject.toJSONString());
        }
    }

}
