import ballerinax/kafka;
import ballerina/io;
import ballerinax/java.jdbc;
// import ballerina/sql;
import ballerina/lang.'int;
// import ballerina/http;

kafka:Producer kafkaProducer = check new (kafka:DEFAULT_URL);

byte[] serializedMsg = "".toBytes();



public function main() returns error? {
    // Initializes the JDBC client.
    // jdbc:Client jdbcClient = check new ("jdbc:h2:file:./target/bbes/java_jdbc", 
    //                                 "rootUser", "rootPass");

    // sql:ExecutionResult result = 
    //         check jdbcClient->execute(`DROP TABLE CourseOutline`);
    //     check createTableSql(jdbcClient);
    
   

    // stream<record {}, error?> resultStream =
    //         jdbcClient->query(`SELECT * FROM Customers`);

    
    // error? e = resultStream.forEach(function(record {} result) {
    //     io:println("Full Customer details1: ", result);
    // });
   // createTableSql( jdbcClient)

    boolean a = true;

    while (a == true){

     string ans = io:readln("Choose an Option: \n1. Lecturer \n2. Student \n3. HOD:\n> ");
        int ans2 = check 'int:fromString(ans);

        if (ans.toLowerAscii() == "lecturer" || ans2 == 1) {
                string name = io:readln("Please Enter The Name: ");
                if (LecName_Pass.hasKey(name)) {
                    string pass = io:readln("Please Enter The Password: ");
                    if(LecName_Pass.get(name) == pass) {
                            string option1 = io:readln("Choose an Option: \n1. Input Information about the course \n2.  Generate and Sign the course outline \n>");
                            int res1 = check 'int:fromString(option1);

                            if( res1 == 1 ) {
                                io:println("Lecturer Information");
                                string lectureName = io:readln("Enter Full Name: ");
                                string Email = io:readln("Enter Email: ");
                                string Location = io:readln("Enter Office Location: ");
                                io:println("");
                                io:println("Course Information");
                                string courseTitle =  io:readln("Enter Course Title: ");
                                string courseCode =  io:readln("Enter Course Code: ");
                                string courseProgramme =  io:readln("Enter Course Programme: ");
                                string courseDesc = io:readln("Enter Course Description: ");
                                string courseOutcomes = io:readln("Enter Learning Outcomes: ");
                                string courseContent = io:readln("Enter Course Content: ");
                                string courseSchedule = io:readln("Enter Course Schedule: ");
                                string courseDates = io:readln("Enter Important Assessment Dates: ");
                                string assessment = io:readln("Enter Assessments: ");

                                    jdbc:Client jdbcClient = check new ("jdbc:h2:file:./target/bbes/java_jdbc", 
                                    "rootUser", "rootPass");
                                json courseTojson = {Name:lectureName, EmailAdress:Email};
                                     

                                check insert(jdbcClient,lectureName,Email,Location,courseTitle,courseCode,courseProgramme,courseDesc,courseOutcomes,courseContent, courseSchedule,courseDates,assessment);

                                        
                                     check jdbcClient.close();
                                     
                                     
                                        byte[] serializedMsg = courseCode.toBytes();
                                    check kafkaProducer->send({
                                topic: "swift1",
                                value: serializedMsg
                                
                                });

                                check kafkaProducer->'flush();
                                     
                               

                            } else if (res1 == 2) {

                                jdbc:Client jdbcClient = check new ("jdbc:h2:file:./target/bbes/java_jdbc", 
                                                                                         "rootUser", "rootPass");
                                string code = io:readln("Enter The Course Code: ");
                                string signature = io:readln("Enter The Signature: ");
                                check updateSignature(jdbcClient, signature,code);
                                    
                                    
                                        check jdbcClient.close();

                                        string hello = code;
                                        byte[] serializedMsg = hello.toBytes();
                                    check kafkaProducer->send({
                                topic: "swift1",
                                value: serializedMsg
                                
                                });

                                check kafkaProducer->'flush();
                                 

                                
                            }

                    } else {
                        io:println("Incorrect Password");
                    }
                } else {
                    io:println("Incorrect Lecturer Name");
                }

        } else if (ans.toLowerAscii() == "student" || ans2 == 2 ) {
                check Student();
        } else if (ans.toLowerAscii() == "HOD" || ans2 == 3) {
                 check HOD();
        } else {
            a = false;
        }
   
    }
   
}





   