declare module "kuzu" {
    export class Database {
        /**
         * Initialize a new Database object. Note that the initialization is done
         * lazily, so the database file is not opened until the first query is
         * executed. To initialize the database immediately, call the `init()`
         * function on the returned object.
         *
         * @param databasePath path to the database file. If the path is not specified, or empty, or equal to
         * `:memory:`, the database will be created in memory.
         * @param bufferManagerSize size of the buffer manager in bytes.
         * @param enableCompression whether to enable compression.
         * @param readOnly if true, database will be opened in read-only mode.
         * @param maxDBSize maximum size of the database file in bytes.
         * @param autoCheckpoint whether to enable automatic checkpointing.
         * @param checkpointThreshold threshold for automatic checkpointing.
         */
        constructor(
            databasePath?: string,
            bufferManagerSize?: number,
            enableCompression?: boolean,
            readOnly?: boolean,
            maxDBSize?: number,
            autoCheckpoint?: boolean,
            checkpointThreshold?: number
        );

        /**
         * Get the version of the library.
         * @returns the version of the library.
         */
        static getVersion(): string;

        /**
         * Get the storage version of the library.
         * @returns the storage version of the library.
         */
        static getStorageVersion(): number;

        /**
         * Initialize the database. Calling this function is optional, as the
         * database is initialized automatically when the first query is executed.
         */
        init(): Promise<void>;

        /**
         * Close the database.
         */
        close(): Promise<void>;
    }

    export class Connection {
        /**
         * Initialize a new Connection object. Note that the initialization is done
         * lazily, so the connection is not initialized until the first query is
         * executed. To initialize the connection immediately, call the `init()`
         * function on the returned object.
         *
         * @param database the database object to connect to.
         * @param numThreads the maximum number of threads to use for query execution.
         */
        constructor(database: Database, numThreads?: number);

        /**
         * Initialize the connection. Calling this function is optional, as the
         * connection is initialized automatically when the first query is executed.
         */
        init(): Promise<void>;

        /**
         * Execute a prepared statement with the given parameters.
         * @param preparedStatement the prepared statement to execute.
         * @param params a plain object mapping parameter names to values.
         * @param progressCallback Optional callback function that is invoked with the progress of the query execution.
         * @returns a promise that resolves to the query result.
         */
        execute(
            preparedStatement: PreparedStatement,
            params?: Record<string, any>,
            progressCallback?: (pipelineProgress: number, numPipelinesFinished: number, numPipelines: number) => void
        ): Promise<QueryResult | QueryResult[]>;

        /**
         * Prepare a statement for execution.
         * @param statement the statement to prepare.
         * @returns a promise that resolves to the prepared statement.
         */
        prepare(statement: string): Promise<PreparedStatement>;

        /**
         * Execute a query.
         * @param statement the statement to execute.
         * @param progressCallback Optional callback function that is invoked with the progress of the query execution.
         * @returns a promise that resolves to the query result.
         */
        query(
            statement: string,
            progressCallback?: (pipelineProgress: number, numPipelinesFinished: number, numPipelines: number) => void
        ): Promise<QueryResult | QueryResult[]>;

        /**
         * Set the maximum number of threads to use for query execution.
         * @param numThreads the maximum number of threads to use for query execution.
         */
        setMaxNumThreadForExec(numThreads: number): void;

        /**
         * Set the timeout for queries. Queries that take longer than the timeout
         * will be aborted.
         * @param timeoutInMs the timeout in milliseconds.
         */
        setQueryTimeout(timeoutInMs: number): void;

        /**
         * Close the connection.
         * Note: Call to this method is optional. The connection will be closed
         * automatically when the object goes out of scope.
         */
        close(): Promise<void>;
    }

    export class PreparedStatement {
        /**
         * Internal constructor. Use `Connection.prepare` to get a
         * `PreparedStatement` object.
         * @param connection the connection object.
         * @param preparedStatement the native prepared statement object.
         */
        constructor(connection: Connection, preparedStatement: any);

        /**
         * Check if the prepared statement is successfully prepared.
         * @returns true if the prepared statement is successfully prepared.
         */
        isSuccess(): boolean;

        /**
         * Get the error message if the prepared statement is not successfully prepared.
         * @returns the error message.
         */
        getErrorMessage(): string;
    }

    export class QueryResult {
        /**
         * Internal constructor. Use `Connection.query` or `Connection.execute`
         * to get a `QueryResult` object.
         * @param connection the connection object.
         * @param queryResult the native query result object.
         */
        constructor(connection: Connection, queryResult: any);

        /**
         * Reset the iterator of the query result to the beginning.
         * This function is useful if the query result is iterated multiple times.
         */
        resetIterator(): void;

        /**
         * Check if the query result has more rows.
         * @returns true if the query result has more rows.
         */
        hasNext(): boolean;

        /**
         * Get the number of rows of the query result.
         * @returns the number of rows of the query result.
         */
        getNumTuples(): number;

        /**
         * Get the next row of the query result.
         * @returns a promise that resolves to the next row of the query result.
         */
        getNext(): Promise<Record<string, any>>;

        /**
         * Iterate through the query result with callback functions.
         * @param resultCallback the callback function that is called for each row of the query result.
         * @param doneCallback the callback function that is called when the iteration is done.
         * @param errorCallback the callback function that is called when there is an error.
         */
        each(
            resultCallback: (row: Record<string, any>) => void,
            doneCallback: () => void,
            errorCallback: (error: Error) => void
        ): void;

        /**
         * Get all rows of the query result.
         * @returns a promise that resolves to all rows of the query result.
         */
        getAll(): Promise<Record<string, any>[]>;

        /**
         * Get all rows of the query result with callback functions.
         * @param resultCallback the callback function that is called with all rows of the query result.
         * @param errorCallback the callback function that is called when there is an error.
         */
        all(
            resultCallback: (rows: Record<string, any>[]) => void,
            errorCallback: (error: Error) => void
        ): void;

        /**
         * Get the data types of the columns of the query result.
         * @returns a promise that resolves to the data types of the columns of the query result.
         */
        getColumnDataTypes(): Promise<string[]>;

        /**
         * Get the names of the columns of the query result.
         * @returns a promise that resolves to the names of the columns of the query result.
         */
        getColumnNames(): Promise<string[]>;

        /**
         * Close the query result.
         */
        close(): void;
    }

    /**
     * Get the version of the library.
     */
    export const VERSION: string;

    /**
     * Get the storage version of the library.
     */
    export const STORAGE_VERSION: number;
} 
