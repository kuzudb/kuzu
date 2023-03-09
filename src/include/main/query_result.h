#pragma once

#include "common/api.h"
#include "common/types/types.h"
#include "kuzu_fwd.h"
#include "processor/result/flat_tuple.h"
#include "query_summary.h"

namespace kuzu {
namespace main {

struct DataTypeInfo {
public:
    DataTypeInfo(common::DataTypeID typeID, std::string name)
        : typeID{typeID}, name{std::move(name)} {}

    common::DataTypeID typeID;
    std::string name;
    std::vector<std::unique_ptr<DataTypeInfo>> childrenTypesInfo;

    static std::unique_ptr<DataTypeInfo> getInfoForDataType(
        const common::DataType& type, const std::string& name);
};

/**
 * @brief QueryResult stores the result of a query execution.
 */
class QueryResult {
    friend class Connection;

public:
    /**
     * @brief Used to create a QueryResult object for the failing query.
     */
    KUZU_API QueryResult();

    explicit QueryResult(const PreparedSummary& preparedSummary);
    /**
     * @brief Deconstructs the QueryResult object.
     */
    KUZU_API ~QueryResult();
    /**
     * @return query is executed successfully or not.
     */
    KUZU_API bool isSuccess() const;
    /**
     * @return error message of the query execution if the query fails.
     */
    KUZU_API std::string getErrorMessage() const;
    /**
     * @return number of columns in query result.
     */
    KUZU_API size_t getNumColumns() const;
    /**
     * @return name of each column in query result.
     */
    KUZU_API std::vector<std::string> getColumnNames();
    /**
     * @return dataType of each column in query result.
     */
    KUZU_API std::vector<common::DataType> getColumnDataTypes();
    /**
     * @return num of tuples in query result.
     */
    KUZU_API uint64_t getNumTuples();
    /**
     * @return query summary which stores the execution time, compiling time, plan and query
     * options.
     */
    KUZU_API QuerySummary* getQuerySummary() const;

    std::vector<std::unique_ptr<DataTypeInfo>> getColumnTypesInfo();
    /**
     * @return whether there are more tuples to read.
     */
    KUZU_API bool hasNext();
    /**
     * @return next flat tuple in the query result.
     */
    KUZU_API std::shared_ptr<processor::FlatTuple> getNext();
    /**
     * @brief writes the query result to a csv file.
     * @param fileName name of the csv file.
     * @param delimiter delimiter of the csv file.
     * @param escapeCharacter escape character of the csv file.
     * @param newline newline character of the csv file.
     */
    KUZU_API void writeToCSV(const std::string& fileName, char delimiter = ',',
        char escapeCharacter = '"', char newline = '\n');
    /**
     * @brief Resets the result tuple iterator.
     */
    KUZU_API void resetIterator();

private:
    void initResultTableAndIterator(std::shared_ptr<processor::FactorizedTable> factorizedTable_,
        const std::vector<std::shared_ptr<binder::Expression>>& columns,
        const std::vector<std::vector<std::shared_ptr<binder::Expression>>>&
            expressionToCollectPerColumn);
    void validateQuerySucceed();

private:
    // execution status
    bool success = true;
    std::string errMsg;

    // header information
    std::vector<std::string> columnNames;
    std::vector<common::DataType> columnDataTypes;
    // data
    std::shared_ptr<processor::FactorizedTable> factorizedTable;
    std::unique_ptr<processor::FlatTupleIterator> iterator;
    std::shared_ptr<processor::FlatTuple> tuple;

    // execution statistics
    std::unique_ptr<QuerySummary> querySummary;
};

} // namespace main
} // namespace kuzu
