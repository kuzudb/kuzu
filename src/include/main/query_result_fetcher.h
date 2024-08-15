#pragma once

#include "main/query_result.h"

namespace kuzu {
namespace main {

struct QueryResultWarningFetcher {
    explicit QueryResultWarningFetcher(QueryResult& result) : queryResult(result) {}

    uint64_t getNumTuples() const { return queryResult.getNumWarnings(); }
    uint64_t getNumColumns() const { return queryResult.getNumWarningColumns(); }
    std::vector<std::string> getColumnNames() const { return queryResult.getWarningColumnNames(); }
    std::vector<common::LogicalType> getColumnDataTypes() const {
        return queryResult.getWarningColumnDataTypes();
    }
    bool hasNext() const { return queryResult.hasNextWarning(); }
    std::shared_ptr<processor::FlatTuple> getNext() { return queryResult.getNextWarning(); }
    void resetIterator() { queryResult.resetWarningIterator(); }
    QuerySummary* getQuerySummary() const { return queryResult.getQuerySummary(); }

    QueryResult& queryResult;
};

struct QueryResultFetcher {
    explicit QueryResultFetcher(QueryResult& result) : queryResult(result) {}

    uint64_t getNumTuples() const { return queryResult.getNumTuples(); }
    uint64_t getNumColumns() const { return queryResult.getNumColumns(); }
    std::vector<std::string> getColumnNames() const { return queryResult.getColumnNames(); }
    std::vector<common::LogicalType> getColumnDataTypes() const {
        return queryResult.getColumnDataTypes();
    }
    bool hasNext() const { return queryResult.hasNext(); }
    std::shared_ptr<processor::FlatTuple> getNext() { return queryResult.getNext(); }
    void resetIterator() { queryResult.resetIterator(); }
    QuerySummary* getQuerySummary() const { return queryResult.getQuerySummary(); }

    QueryResult& queryResult;
};

} // namespace main
} // namespace kuzu
