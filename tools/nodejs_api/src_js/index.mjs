import kuzu from "./index.js";

// Re-export everything from the CommonJS module
export const Database = kuzu.Database;
export const Connection = kuzu.Connection;
export const PreparedStatement = kuzu.PreparedStatement;
export const QueryResult = kuzu.QueryResult;
export default kuzu;