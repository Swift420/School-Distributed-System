
import ballerinax/java.jdbc;
import ballerina/sql;
import ballerina/io;

map<string> LecName_Pass = { John: "Apple", Jane: "Orange",
        Steve: "PC", Alex: "havana" };

map<string> HODName_Pass = { Mike: "pie", Kane: "aa",
        Adam: "iLoveApple", James: "phone1" };


              
    function createTableSql(jdbc:Client jdbcClient) returns sql:Error? {

    sql:ExecutionResult result = 
        check jdbcClient->execute(`CREATE TABLE CourseOutline(courseCode varchar(50) PRIMARY KEY, lectureName  VARCHAR(250), email  VARCHAR(250),
            location varchar(200), title varchar(200), programme  VARCHAR(200),description  VARCHAR(200), Outcomes  VARCHAR(500),content  VARCHAR(800),
            schedule  VARCHAR(500), importantDates varchar(500), assessments  VARCHAR(500), signature varchar(50), HODapproved varchar(50) )`);

    
}

function insert(jdbc:Client jdbcClient, string name,string email,string Location, string courseTitle, string courseCode, string courseProgramme, string courseDesc, string courseOutcomes, string courseContent, string courseSchedule, string courseDates, string assessment ) returns sql:Error? {
    

    sql:ParameterizedQuery query = `INSERT INTO CourseOutline(courseCode , lectureName  , email  ,
            location , title , programme ,description  , Outcomes  ,content ,
            schedule  , importantDates , assessments  )
                                  VALUES (${'courseCode}, ${name}, ${email}, ${Location}, ${courseTitle}, ${courseProgramme}, ${courseDesc}, ${courseOutcomes}, ${courseContent}, ${courseSchedule}, ${courseDates}, ${assessment});`;
sql:ExecutionResult result2 = check jdbcClient->execute(query);

    
}
    

function updateApproval(jdbc:Client jdbcClient, string approve, string code) returns sql:Error? {

    sql:ExecutionResult result = 
            check jdbcClient->execute(`UPDATE CourseOutline SET HODapproved = ${approve} WHERE courseCode = ${code}`);

    
}    

function updateSignature(jdbc:Client jdbcClient, string sign, string code) returns sql:Error? {

    sql:ExecutionResult result = 
            check jdbcClient->execute(`UPDATE CourseOutline SET signature = ${sign} WHERE courseCode = ${code}`);

    
}


function HOD() returns error? {
    string name = io:readln("Please Enter Your Name: ");
                if (HODName_Pass.hasKey(name)) {
                    string pass = io:readln("Please Enter Your Password: ");
                    if(HODName_Pass.get(name) == pass) {
                            string option1 = io:readln("Choose an Option: \n1. Input Information about the course \n2.  View All Generated Course Outlines \n3. Approve a course outline\n>");
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
                               
                                     

                                check insert(jdbcClient,lectureName,Email,Location,courseTitle,courseCode,courseProgramme,courseDesc,courseOutcomes,courseContent, courseSchedule,courseDates,assessment);

                                        
                                     check jdbcClient.close();
                                     
                                     
                                byte[] serializedMsg = courseCode.toBytes();
                                check kafkaProducer->send({
                                topic: "order66",
                                value: serializedMsg
                                
                                });

                                check kafkaProducer->'flush();
                                     
                               

                            } else if (res1 == 2) {

                                jdbc:Client jdbcClient = check new ("jdbc:h2:file:./target/bbes/java_jdbc", 
                                                                                         "rootUser", "rootPass");
                                // string code = io:readln("Enter The Course Code: ");
                                // string signature = io:readln("Enter The Signature: ");
                                // check updateSignature(jdbcClient, signature,code);
                                    
                                    
                                  
     check jdbcClient.close();

                                        string hello = "all";
                                        byte[] serializedMsg = hello.toBytes();
                                    check kafkaProducer->send({
                                topic: "order66",
                                value: hello.toBytes()
                                
                                });

                                check kafkaProducer->'flush();
                                 

                                
                            } else if( res1 == 3) {
                                jdbc:Client jdbcClient = check new ("jdbc:h2:file:./target/bbes/java_jdbc", 
                                                                                         "rootUser", "rootPass");
                                
                                
                                string code = io:readln("Enter The Course Code For Course Outline to be approved: ");
                                 string hello = "Yes";
                                // string signature = io:readln("Enter The Signature: ");
                                
                                check updateApproval(jdbcClient, hello,code);

                                        check jdbcClient.close();

                                       
                                        byte[] serializedMsg = hello.toBytes();
                                    check kafkaProducer->send({
                                topic: "order66",
                                value: serializedMsg
                                
                                });

                                check kafkaProducer->'flush();

                                io:println("Successfully Approved");
                            }

                    } else {
                        io:println("Incorrect Password");
                    }
                } else {
                    io:println("Incorrect HOD Name");
                }

                
}



function Student() returns error? {
   
                            string option1 = io:readln("Choose an Option: \n1. View Course Outline \n2.  Acknowledge a Course Outline\n>");
                            int res1 = check 'int:fromString(option1);

                             if (res1 == 1) {

                                jdbc:Client jdbcClient = check new ("jdbc:h2:file:./target/bbes/java_jdbc", 
                                                                                         "rootUser", "rootPass");
                                string code = io:readln("Enter The Course Code: ");
                                // string signature = io:readln("Enter The Signature: ");
                                // check updateSignature(jdbcClient, signature,code);
                                    
                                    
                                        check jdbcClient.close();

                                        string hello = code;
                                        byte[] serializedMsg = hello.toBytes();
                                    check kafkaProducer->send({
                                topic: "duh",
                                value: serializedMsg
                                
                                });

                                check kafkaProducer->'flush();
                                 

                                
                            } else if( res1 == 2) {
                                // jdbc:Client jdbcClient = check new ("jdbc:h2:file:./target/bbes/java_jdbc", 
                                //                                                          "rootUser", "rootPass");
                                
                                
                                // string code = io:readln("Enter The Course Code For Course Outline to be approved: ");
                                //  string hello = "Yes";
                                // // string signature = io:readln("Enter The Signature: ");
                                
                                // check updateApproval(jdbcClient, hello,code);

                                //         check jdbcClient.close();

                                       
                                //         byte[] serializedMsg = hello.toBytes();
                                //     check kafkaProducer->send({
                                // topic: "order66",
                                // value: serializedMsg
                                
                                // });

                                // check kafkaProducer->'flush();

                                // io:println("Successfully Approved");
                            }

  } 

                
