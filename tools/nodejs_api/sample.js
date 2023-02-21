// Make sure the test directory is removed as it will be recreated
const fs = require("fs");
const {Database, Connection, QueryResult} = require("./build/kuzu");

try {
  fs.rmSync("./test", { recursive: true });
} catch (e) {
  // ignore
}

function executeAllCallback(queryResult) {
    queryResult.all( result => {
        console.log("All result received Callback");
        console.log(result);
    });
}

function executeAllPromise(queryResult) {
    queryResult.all().then( result => {
        console.log("All result received Promise");
        console.log(result);
    }).catch( error => {
        console.log("All with Promise failed");
        console.log(error);
    });
}

// Basic Case with all callback
const database = new Database("test", 1000000000);
console.log("The database looks like: ", database);
const connection = new Connection(database);
console.log ("The connection looks like: ", connection);
connection.execute("create node table person (ID INt64, fName StRING, gender INT64, isStudent BoOLEAN, isWorker BOOLEAN, age INT64, eyeSight DOUBLE, birthdate DATE, registerTime TIMESTAMP, lastJobDuration interval, workedHours INT64[], usedNames STRING[], courseScoresPerTerm INT64[][], PRIMARY KEY (ID));", executeAllCallback);
connection.execute("COPY person FROM \"../../dataset/tinysnb/vPerson.csv\" (HEADER=true);", executeAllPromise);
const executeQuery = "MATCH (a:person) RETURN a.fName, a.age, a.eyeSight, a.isStudent;";
connection.execute(executeQuery, executeAllCallback);
connection.execute(executeQuery, executeAllPromise);

// Extensive Case
database.resizeBufferManager(2000000000);
connection.setMaxNumThreadForExec(2);
connection.execute(executeQuery, executeAllCallback);
console.log(connection.getNodePropertyNames("person"));

// Execute with each callback
connection.execute(executeQuery,  result => {
    result.each(
        row => { console.log(row); },
        () => { console.log("all of the each's are done callback"); }
    );
});

// Execute with promise + await
connection.execute(executeQuery).then(queryResult => {
    queryResult.all( result => {
        console.log("All result received for execution with a promise");
        console.log(result);
    });
}).catch(error => {
    console.log("Execution with a promise failed");
    console.log(error);
});

async function asyncAwaitExecute(executeQuery) {
    const queryResult = await connection.execute(executeQuery);
    return queryResult;
}
asyncAwaitExecute(executeQuery).then(queryResult => {
    queryResult.all( result => {
        console.log("All result received for execution with await");
        console.log(result);
    });
}).catch(error => {
    console.log("Execution with await failed");
    console.log(error);
});
