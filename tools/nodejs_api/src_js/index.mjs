import kuzu from "./index.js";

// Re-export everything from the CommonJS module
export const Database = kuzu.Database;
export const Connection = kuzu.Connection;
export const PreparedStatement = kuzu.PreparedStatement;
export const QueryResult = kuzu.QueryResult;
export const VERSION = kuzu.VERSION;
export const STORAGE_VERSION = kuzu.STORAGE_VERSION;
export default kuzu;
