#pragma once

#include "main/query_result.h"
#include <concepts>

namespace kuzu {
namespace main {

template<typename T>
concept QueryResultFetcher = requires(T fetcher) {
    { fetcher.getNumTuples() } -> std::convertible_to<uint64_t>;
    { fetcher.getNumColumns() } -> std::convertible_to<uint64_t>;
    { fetcher.getColumnNames() } -> std::same_as<std::vector<std::string>>;
    { fetcher.getColumnDataTypes() } -> std::same_as<std::vector<common::LogicalType>>;
    { fetcher.hasNext() } -> std::same_as<bool>;
    { fetcher.getNext() } -> std::same_as<std::shared_ptr<processor::FlatTuple>>;
    { fetcher.resetIterator() };
    { fetcher.getQuerySummary() } -> std::convertible_to<QuerySummary*>;
};

struct FetchQueryWarnings {
    explicit FetchQueryWarnings(QueryResult& result) : queryResult(result) {}

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
static_assert(QueryResultFetcher<FetchQueryWarnings>);

struct FetchQueryResults {
    explicit FetchQueryResults(QueryResult& result) : queryResult(result) {}

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

static_assert(QueryResultFetcher<FetchQueryResults>);

} // namespace main
} // namespace kuzu
