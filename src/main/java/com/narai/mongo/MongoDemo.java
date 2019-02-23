package com.narai.mongo;

import org.bson.Document;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.*;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;

/**
 * @author: kaze
 * @date: 2019-02-23
 */
@Component
public class MongoDemo {

    @Resource
    private MongoTemplate mongoTemplate;

    public void query() {
        {
            Query query = new Query();
            query.addCriteria(Criteria.where("_id").gte("5c661458fadb9213373e879b"));
//            query = new BasicQuery("{_id:{$gte:ObjectId(\"5c661458fadb9213373e879b\")}}");
            query.limit(10000);
            List<Object> list = mongoTemplate.find(query, Object.class);
        }
        {
            BulkOperations bulkOps = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED, "collectionName");
            Query query = new Query();
            query.addCriteria(Criteria.where("_id").is("5c661458fadb9213373e879b"));
            Update update = new Update();
            update.set("key", "value");
            update.unset("key");
            bulkOps.upsert(query, update);
            // 空会报异常
            bulkOps.execute();
        }
        {
            MatchOperation match = Aggregation.match(Criteria.where("_id").is("5c661458fadb9213373e879b"));
            GroupOperation group = Aggregation.group(Fields.fields("fieldName")).count().as("count");
            Aggregation agg = Aggregation.newAggregation(match, group).withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());
            AggregationResults<Object> output = mongoTemplate.aggregate(agg, "collectionName", Object.class);
            Document response = output.getRawResults();
        }
    }

}
