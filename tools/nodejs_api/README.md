## Kùzu Node.js API

### Install

```
npm i kuzu
```

### Example usage

```js
// Import library
const kuzu = require("kuzu");

(async () => {
  // Create an empty database and connect to it
  const db = new kuzu.Database("./test");
  const conn = new kuzu.Connection(db);

  // Create the tables
  await conn.query(
    "CREATE NODE TABLE User(name STRING, age INT64, PRIMARY KEY (name))"
  );
  await conn.query(
    "CREATE NODE TABLE City(name STRING, population INT64, PRIMARY KEY (name))"
  );
  await conn.query("CREATE REL TABLE Follows(FROM User TO User, since INT64)");
  await conn.query("CREATE REL TABLE LivesIn(FROM User TO City)");

  // Load the data
  await conn.query('COPY User FROM "user.csv"');
  await conn.query('COPY City FROM "city.csv"');
  await conn.query('COPY Follows FROM "follows.csv"');
  await conn.query('COPY LivesIn FROM "lives-in.csv"');

  const queryResult = await conn.query("MATCH (u:User) RETURN u.name, u.age;");

  // Get all rows from the query result
  const rows = await queryResult.getAll();

  // Print the rows
  for (const row of rows) {
    console.log(row);
  }
})();
```

The dataset used in this example can be found [here](https://github.com/kuzudb/kuzu/tree/master/dataset/demo-db/csv).

### Local development (for contributors)

#### Install dependency

```
npm i --include=dev
```

#### Build

```
npm run build
```

#### Run test

```
npm test
```

#### Package

We use the approach of packing all the prebuilt binaries into a single file, this approach is inspired by [prebuildify](https://github.com/prebuild/prebuildify).

> All prebuilt binaries are shipped inside the package that is published to npm, which means there's no need for a separate download step like you find in [`prebuild`](https://github.com/prebuild/prebuild). The irony of this approach is that it is faster to download all prebuilt binaries for every platform when they are bundled than it is to download a single prebuilt binary as an install script.

During the installation, the package will check the platform and architecture of the current machine, and extract the corresponding prebuilt binaries from the package. If no prebuilt binaries are available for the current platform and architecture, it will try to build from source code. Note that building from source code requires CMake(>=3.15), Python 3, and a compiler that supports `C++20`.

We have configured our CI to build prebuilt binaries for Linux and macOS. To package the prebuilt binaries, put the prebuilt binaries under `prebuilt` directory, the name of the prebuilt binaries should be in the format of `kuzujs-${platform}-${arch}.node`, then run:

```
node package
```

If no prebuilt binaries are provided, the packaging script will still work, but it will create a source-only package, which means it will always be built from source code when installing the package.

#### Publish

The created tarball can be published to npm. Please refer to [npm documentation](https://docs.npmjs.com/cli/v9/commands/npm-publish) for more details.
