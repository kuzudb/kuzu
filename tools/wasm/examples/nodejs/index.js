const kuzu = require("kuzu-wasm/nodejs");
const fs = require("fs/promises");

let db;
let conn;

const initDatabase = async () => {
  console.log("Initializing database...");
  // Create a new database and connection
  db = new kuzu.Database(":memory:", 1 << 30 /* 1GB */);
  conn = new kuzu.Connection(db, 4);

  // Load the schema from local dataset file
  const schema = (await fs.readFile("../../../../dataset/demo-db/csv/schema.cypher"))
    .toString()
    .split("\n");
  for (const line of schema) {
    if (line.trim().length === 0) {
      continue;
    }
    console.log("Executing: ", line);
    const queryResult = await conn.query(line);
    console.log((await queryResult.toString()));
    await queryResult.close();
  }

  // Load the data from local dataset file
  const copy = (await fs.readFile("../../../../dataset/demo-db/csv/copy.cypher"))
    .toString()
    .split("\n");

  for (const line of copy) {
    if (line.trim().length === 0) {
      continue;
    }
    const statement = line.replace("dataset/demo-db/csv", "../../../../dataset/demo-db/csv");
    console.log("Executing: ", statement);
    const queryResult = await conn.query(statement);
    console.log((await queryResult.toString()));
    await queryResult.close();
  }
};

const query = async () => {
  // Run some queries
  let queryString = "MATCH (u:User) -[f:Follows]-> (v:User) RETURN u.name, f.since, v.name";
  console.log("Executing query: " + queryString);
  let queryResult = await conn.query(queryString);
  let rows = await queryResult.getAllObjects();
  console.log("Query result:");
  for (const row of rows) {
    console.log(`User ${row['u.name']} follows ${row['v.name']} since ${row['f.since']}`);
  }
  await queryResult.close();

  console.log("");

  queryString = "MATCH (u:User) -[l:LivesIn]-> (c:City) RETURN u.name, c.name";
  console.log("Executing query: " + queryString);
  queryResult = await conn.query(queryString);
  rows = await queryResult.getAllObjects();
  console.log("Query result:");
  // Print the rows
  for (const row of rows) {
    console.log(`User ${row['u.name']} lives in ${row['c.name']}`);
  }
  await queryResult.close();
};


const cleanup = async () => {
  console.log("Cleaning up...");
  await conn.close();
  await db.close();
  await kuzu.close();
};

(async () => {
  await initDatabase();
  await query();
  await cleanup();
  console.log("Done!");
})();
