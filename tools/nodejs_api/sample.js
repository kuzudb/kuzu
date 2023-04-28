// Make sure the test directory is removed as it will be recreated
const fs = require("fs");
const {Database, Connection} = require("./build/kuzu");

try {
  fs.rmSync("./testDb", { recursive: true });
} catch (e) {
  // ignore
}

async function executeAllCallback(err, queryResult) {
    if (err) {
        console.log(err);
    } else {
        await queryResult.all({"callback": (err, result) => {
            if (err) {
                console.log("All result with Callback failed");
                console.log(err);
            } else {
                console.log(result);
                console.log("All result received Callback");
            }
        }});
    }
}

async function executeAllPromise(err, queryResult) {
    if (err) {
        console.log(err);
    } else {
        await queryResult.all().then(result => {
            console.log(result);
            console.log("All result received Promise");
        }).catch(error => {
            console.log("All with Promise failed");
            console.log(error);
        });
    }
}

// Basic Case with all callback
const database = new Database("testDb", 1000000000);
console.log("The database looks like: ", database);
const connection = new Connection(database);
console.log ("The connection looks like: ", connection);
connection.execute("create node table person (ID INt64, fName StRING, gender INT64, isStudent BoOLEAN, isWorker BOOLEAN, age INT64, eyeSight DOUBLE, birthdate DATE, registerTime TIMESTAMP, lastJobDuration interval, workedHours INT64[], usedNames STRING[], courseScoresPerTerm INT64[][], grades INT64[], height DOUBLE, PRIMARY KEY (ID));", {"callback": executeAllCallback}).then(async r => {
    await connection.execute("COPY person FROM \"../../dataset/tinysnb/vPerson.csv\" (HEADER=true);", {"callback": executeAllPromise})
    const executeQuery = "MATCH (a:person) RETURN a.fName, a.age, a.eyeSight, a.isStudent;";
    const parameterizedExecuteQuery = "MATCH (a:person) WHERE a.age > $1 and a.isStudent = $2 and a.fName < $3  RETURN a.fName, a.age, a.eyeSight, a.isStudent;";

    connection.execute(executeQuery, {"callback": executeAllPromise});
    connection.execute(parameterizedExecuteQuery, {
        "callback": executeAllPromise,
        "params": [["1", 29], ["2", true], ["3", "B"]]
    });

    // Extensive Case
    connection.setMaxNumThreadForExec(2);
    connection.execute(executeQuery, {"callback": executeAllCallback});

    // Execute with each callback
    connection.execute(executeQuery, {
        "callback": async (err, result) => {
            if (err) {
                console.log(err);
            } else {
                await result.each(
                    (err, rowResult) => {
                        if (err) {
                            console.log(err)
                        } else {
                            console.log(rowResult);
                        }
                    },
                    () => {
                        console.log("all of the each's are done callback");
                    }
                );
            }
        }
    });

    // Execute with promise + await
    connection.execute(executeQuery).then(async queryResult => {
        await queryResult.all({
            "callback": (err, result) => {
                if (err) {
                    console.log(err);
                } else {
                    console.log(result);
                    console.log("All result received for execution with a promise");
                }
            }
        });
    }).catch(error => {
        console.log("Execution with a promise failed");
        console.log(error);
    });

    async function asyncAwaitExecute(executeQuery) {
        const queryResult = await connection.execute(executeQuery);
        return queryResult;
    }

    await asyncAwaitExecute(executeQuery).then(async queryResult => {
        await queryResult.all({
            "callback": (err, result) => {
                if (err) {
                    console.log(err);
                } else {
                    console.log(result);
                    console.log("All result received for execution with await");
                }
            }
        });
    }).catch(error => {
        console.log("Execution with await failed");
        console.log(error);
    });
});