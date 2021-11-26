
import ballerinax/kafka;
import ballerina/io;
import ballerinax/java.jdbc;
import ballerina/sql;
import ballerina/log;





kafka:ConsumerConfiguration consumerConfigs = {
    groupId: "group-id",
    topics: ["duh"],

    pollingInterval: 1,
    autoCommit: false
};

listener kafka:Listener kafkaListener =
        new ("localhost:9095", consumerConfigs);

service kafka:Service on kafkaListener {


    remote function onConsumerRecord(kafka:Caller caller,
                                kafka:ConsumerRecord[] records) returns error? {
        foreach var kafkaRecord in records {
            check processKafkaRecord(kafkaRecord);
            
        }
        
       
        kafka:Error? commitResult = caller->commit();

        if commitResult is error {
            log:printError("Error occurred while committing the " +
                "offsets for the consumer ", 'error = commitResult);
        }
    }
}
function processKafkaRecord(kafka:ConsumerRecord kafkaRecord) returns error? {
    
    byte[] value = kafkaRecord.value;
   
   
    
    string messageContent = check string:fromBytes(value);

         io:println("");
    
        check showOutline(messageContent);
    

         io:println("");
    
  
     
}


function showOutline(string val) returns sql:Error? {
    jdbc:Client jdbcClient = check new ("jdbc:h2:file:C:/Users/Apollos/dsa_codes/DSA3/client/target/bbes/java_jdbc", 
        "rootUser", "rootPass");

         


        stream<record {}, error?> resultStream =
            jdbcClient->query(`SELECT * FROM CourseOutline WHERE courseCode = ${val} `);

    error? e = resultStream.forEach(function(record {} result) {
        io:println(result);
    });
     check jdbcClient.close();
}


function showAllOutline() returns sql:Error? {
    jdbc:Client jdbcClient = check new ("jdbc:h2:file:C:/Users/Apollos/dsa_codes/DSA3/client/target/bbes/java_jdbc", 
        "rootUser", "rootPass");

         


        stream<record {}, error?> resultStream =
            jdbcClient->query(`SELECT * FROM CourseOutline  `);

    error? e = resultStream.forEach(function(record {} result) {
        io:println(result);
    });
     check jdbcClient.close();
}

