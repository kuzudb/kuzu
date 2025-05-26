
/**
 * A nullable type that can be either T or null.
 */
export type Nullable<T> = T | null;

/**
 * A callback function pattern used throughout the API.
 * @template T The type of the result value (defaults to void)
 */
export type Callback<T = void> = (error: Error | null, result?: T) => void;

/**
 * Callback for query execution progress updates.
 * @param pipelineProgress Progress of the current pipeline (0-100)
 * @param numPipelinesFinished Number of pipelines completed
 * @param numPipelines Total number of pipelines
 */
export type ProgressCallback = (
    pipelineProgress: number,
    numPipelinesFinished: number,
    numPipelines: number
) => void;

/**
 * Represents a node ID in the graph database.
 */
export interface NodeID {
    /** The offset of the node in its table */
    offset: number;
    /** The table ID the node belongs to */
    table: number;
}

/**
 * Represents a node value in the graph.
 */
export interface NodeValue {
    /** The label of the node (type) */
    _label: string | null;
    /** The ID of the node */
    _id: NodeID | null;
    /** Additional properties of the node */
    [key: string]: any;
}

/**
 * Represents a relationship (edge) value in the graph.
 */
export interface RelValue {
    /** The source node ID */
    _src: NodeID | null;
    /** The destination node ID */
    _dst: NodeID | null;
    /** The relationship type/label */
    _label: string | null;
    /** The relationship ID */
    _id: any;
    /** Additional properties of the relationship */
    [key: string]: any;
}

/**
 * Represents a recursive relationship value (path) in the graph.
 */
export interface RecursiveRelValue {
    /** Array of nodes in the path */
    _nodes: any[];
    /** Array of relationships in the path */
    _rels: any[];
}

/**
 * Union type representing all possible value types in Kuzu.
 */
export type KuzuValue =
    | null
    | boolean
    | number
    | bigint
    | string
    | Date
    | NodeValue
    | RelValue
    | RecursiveRelValue
    | KuzuValue[]
    | { [key: string]: KuzuValue };

/**
 * Configuration options for the database system.
 */
export interface SystemConfig {
    /** Size of the buffer pool in bytes */
    bufferPoolSize?: number;
    /** Whether to enable compression */
    enableCompression?: boolean;
    /** Whether to open the database in read-only mode */
    readOnly?: boolean;
    /** Maximum size of the database in bytes */
    maxDBSize?: number;
    /** Whether to enable automatic checkpoints */
    autoCheckpoint?: boolean;
    /** Threshold for automatic checkpoints */
    checkpointThreshold?: number;
}

/**
 * Represents a Kuzu database instance.
 */
export class Database {
    /**
     * Constructs a new Database instance.
     * @param databasePath Path to the database directory
     * @param bufferPoolSize Size of the buffer pool in bytes
     * @param enableCompression Whether to enable compression
     * @param readOnly Whether to open in read-only mode
     * @param maxDBSize Maximum size of the database in bytes
     * @param autoCheckpoint Whether to enable automatic checkpoints
     * @param checkpointThreshold Threshold for automatic checkpoints
     */
    constructor(
        databasePath?: string,
        bufferPoolSize?: number,
        enableCompression?: boolean,
        readOnly?: boolean,
        maxDBSize?: number,
        autoCheckpoint?: boolean,
        checkpointThreshold?: number
    );

    /**
     * Constructs a new Database instance with configuration object.
     * @param databasePath Path to the database directory
     * @param config Configuration options for the database
     */
    constructor(
        databasePath?: string,
        config?: SystemConfig
    );

    /**
     * Initialize the database asynchronously. Calling this function is optional, as the
     * database is initialized automatically when the first query is executed.
     * @param callback Callback to be called when initialization completes
     */
    initAsync(callback: Callback): void;

    /**
     * Initialize the database. Calling this function is optional, as the
     * database is initialized automatically when the first query is executed.
     * @returns Promise that resolves when initialization completes
     */
    init(): Promise<void>;

    /**
     * Close the database and release resources.
     */
    close(): void;

    /**
     * Get the version of the Kuzu library.
     * @returns The version string of the library
     */
    static getVersion(): string;

    /**
     * Get the storage version of the Kuzu library.
     * @returns The storage version the library
     */
    static getStorageVersion(): bigint;
}

/**
 * Represents a connection to a Kuzu database.
 */
export class Connection {
    /**
     * Creates a new connection to the specified database.
     * @param database The database instance to connect to
     */
    constructor(database: Database);

    /**
     * Initialize the connection asynchronously.
     * @param callback Callback to be called when initialization completes
     */
    initAsync(callback: Callback): void;

    /**
     * Initialize the connection.
     * @returns Promise that resolves when initialization completes
     */
    init(): Promise<void>;

    /**
     * Set the maximum number of threads for query execution.
     * @param numThreads The number of threads to use
     */
    setMaxNumThreadForExec(numThreads: number): void;

    /**
     * Set the query timeout in milliseconds.
     * @param timeoutInMS Timeout in milliseconds
     */
    setQueryTimeout(timeoutInMS: number): void;

    /**
     * Close the connection.
     */
    close(): void;

    /**
     * Execute a prepared statement asynchronously.
     * @param preparedStatement The prepared statement to execute
     * @param queryResult The query result object to store results
     * @param params Parameters for the query (as object or entries array)
     * @param callback Callback to be called when execution completes
     * @param progressCallback Optional progress callback
     */
    executeAsync(
        preparedStatement: PreparedStatement,
        queryResult: QueryResult,
        params: Record<string, KuzuValue> | [string, KuzuValue][],
        callback: Callback,
        progressCallback?: ProgressCallback
    ): void;

    /**
     * Execute a raw query string asynchronously.
     * @param statement The query string to execute
     * @param queryResult The query result object to store results
     * @param callback Callback to be called when execution completes
     * @param progressCallback Optional progress callback
     */
    queryAsync(
        statement: string,
        queryResult: QueryResult,
        callback: Callback,
        progressCallback?: ProgressCallback
    ): void;

    /**
     * Execute a prepared statement synchronously.
     * @param preparedStatement The prepared statement to execute
     * @param queryResult The query result object to store results
     * @param params Parameters for the query (as object or entries array)
     */
    executeSync(
        preparedStatement: PreparedStatement,
        queryResult: QueryResult,
        params: Record<string, KuzuValue> | [string, KuzuValue][]
    ): void;

    /**
     * Execute a raw query string synchronously.
     * @param statement The query string to execute
     * @param queryResult The query result object to store results
     */
    querySync(statement: string, queryResult: QueryResult): void;
}

/**
 * Represents a prepared statement for efficient query execution.
 */
export class PreparedStatement {
    /**
     * Creates a new prepared statement.
     * @param connection The connection to prepare the statement on
     * @param query The query string to prepare
     */
    constructor(connection: Connection, query: string);

    /**
     * Initialize the prepared statement asynchronously.
     * @param callback Callback to be called when initialization completes
     */
    initAsync(callback: Callback): void;

    /**
     * Initialize the prepared statement synchronously.
     */
    initSync(): void;

    /**
      * Check if the statement was prepared successfully.
      * @returns True if preparation was successful
      */
    isSuccess(): boolean;

    /**
     * Get the error message if preparation failed.
     * @returns The error message or empty string if successful
     */
    getErrorMessage(): string;
}

/**
 * Represents the results of a query execution.
 */
export class QueryResult {
    constructor();

    /**
     * Reset the iterator for reading results.
     */
    resetIterator(): void;

    /**
     * Check if there are more rows to read.
     * @returns True if more rows are available
     */
    hasNext(): boolean;

    /**
     * Check if there are more query result chunks available.
     * @returns True if more result chunks are available
     */
    hasNextQueryResult(): boolean;

    /**
     * Get the next query result chunk asynchronously.
     * @param nextQueryResult The result object to store the next chunk
     * @param callback Callback to be called when the next chunk is ready
     */
    getNextQueryResultAsync(nextQueryResult: QueryResult, callback: Callback): void;

    /**
     * Get the next query result chunk synchronously.
     * @param nextQueryResult The result object to store the next chunk
     */
    getNextQueryResultSync(nextQueryResult: QueryResult): void;

    /**
     * Get the number of tuples (rows) in the result.
     * @returns The number of rows
     */
    getNumTuples(): number;

    /**
     * Get the next row asynchronously.
     * @param callback Callback receiving the next row or null if no more rows
     */
    getNextAsync(callback: Callback<Record<string, KuzuValue>>): void;

    /**
     * Get the next row synchronously.
     * @returns The next row or null if no more rows
     */
    getNextSync(): Record<string, KuzuValue> | null;

    /**
     * Get the column data types asynchronously.
     * @param callback Callback receiving the array of data type strings
     */
    getColumnDataTypesAsync(callback: Callback<string[]>): void;

    /**
     * Get the column data types synchronously.
     * @returns Array of data type strings
     */
    getColumnDataTypesSync(): string[];

    /**
     * Get the column names asynchronously.
     * @param callback Callback receiving the array of column names
     */
    getColumnNamesAsync(callback: Callback<string[]>): void;

    /**
     * Get the column names synchronously.
     * @returns Array of column names
     */
    getColumnNamesSync(): string[];

    /**
     * Close the result set and release resources.
     */
    close(): void;
}

/**
 * Default export for the Kuzu module.
 */
declare const kuzu: {
    Database: typeof Database;
    Connection: typeof Connection;
    PreparedStatement: typeof PreparedStatement;
    QueryResult: typeof QueryResult;
    VERSION: string;
    STORAGE_VERSION: bigint;
};

export default kuzu;
