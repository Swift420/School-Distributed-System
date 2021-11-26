
import ballerinax/kafka;
import ballerina/io;
import ballerinax/java.jdbc;
import ballerina/sql;
import ballerina/log;





kafka:ConsumerConfiguration consumerConfigs = {
    groupId: "group-id",
    topics: ["order66"],

    pollingInterval: 1,
    autoCommit: false
};
final string DEFAULT_URL = "localhost:9095";

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

    if (messageContent == "all") {
         io:println("");
            check showAllOutline();
             io:println("");
    } else {
         io:println("");
        check showOutline(messageContent);
         io:println("");
    }

   
    
  
     
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

function createTableSql(jdbc:Client jdbcClient) returns sql:Error? {

    sql:ExecutionResult result = 
        check jdbcClient->execute(`CREATE TABLE CourseOutline(courseCode varchar(50) PRIMARY KEY, lectureName  VARCHAR(250), email  VARCHAR(250),
            location varchar(200), title varchar(200), programme  VARCHAR(200),description  VARCHAR(200), Outcomes  VARCHAR(500),content  VARCHAR(800),
            schedule  VARCHAR(500), importantDates varchar(500), assessments  VARCHAR(500), signature varchar(50), HODapproved varchar(50) )`);

    
}