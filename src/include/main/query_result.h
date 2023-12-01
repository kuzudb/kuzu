#pragma once

#include "common/api.h"
#include "common/arrow/arrow.h"
#include "common/types/types.h"
#include "kuzu_fwd.h"
#include "processor/result/flat_tuple.h"
#include "query_summary.h"

namespace kuzu {
namespace main {

struct DataTypeInfo {
public:
    DataTypeInfo(common::LogicalTypeID typeID, std::string name)
        : typeID{typeID}, name{std::move(name)}, numValuesPerList{0} {}

    common::LogicalTypeID typeID;
    std::string name;
    std::vector<std::unique_ptr<DataTypeInfo>> childrenTypesInfo;
    // Used by fixedList only.
    uint64_t numValuesPerList;

    static std::unique_ptr<DataTypeInfo> getInfoForDataType(
        const common::LogicalType& type, const std::string& name);
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
    KUZU_API std::vector<std::string> getColumnNames() const;
    /**
     * @return dataType of each column in query result.
     */
    KUZU_API std::vector<common::LogicalType> getColumnDataTypes() const;
    /**
     * @return num of tuples in query result.
     */
    KUZU_API uint64_t getNumTuples() const;
    /**
     * @return query summary which stores the execution time, compiling time, plan and query
     * options.
     */
    KUZU_API QuerySummary* getQuerySummary() const;

    std::vector<std::unique_ptr<DataTypeInfo>> getColumnTypesInfo() const;
    /**
     * @return whether there are more tuples to read.
     */
    KUZU_API bool hasNext() const;
    /**
     * @return next flat tuple in the query result.
     */
    KUZU_API std::shared_ptr<processor::FlatTuple> getNext();

    KUZU_API std::string toString();
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

    processor::FactorizedTable* getTable() { return factorizedTable.get(); }

    /**
     * @brief Returns the arrow schema of the query result.
     * @return datatypes of the columns as an arrow schema
     *
     * It is the caller's responsibility to call the release function to release the underlying data
     * If converting to another arrow type, this this is usually handled automatically.
     */
    KUZU_API std::unique_ptr<ArrowSchema> getArrowSchema() const;

    /**
     * @brief Returns the next chunk of the query result as an arrow array.
     * @param chunkSize number of tuples to return in the chunk.
     * @return An arrow array representation of the next chunkSize tuples of the query result.
     *
     * The ArrowArray internally stores an arrow struct with fields for each of the columns.
     * This can be converted to a RecordBatch with arrow's ImportRecordBatch function
     *
     * It is the caller's responsibility to call the release function to release the underlying data
     * If converting to another arrow type, this this is usually handled automatically.
     */
    KUZU_API std::unique_ptr<ArrowArray> getNextArrowChunk(int64_t chunkSize);

private:
    void initResultTableAndIterator(std::shared_ptr<processor::FactorizedTable> factorizedTable_,
        const std::vector<std::shared_ptr<binder::Expression>>& columns);
    void validateQuerySucceed() const;

private:
    // execution status
    bool success = true;
    std::string errMsg;

    // header information
    std::vector<std::string> columnNames;
    std::vector<common::LogicalType> columnDataTypes;
    // data
    std::shared_ptr<processor::FactorizedTable> factorizedTable;
    std::unique_ptr<processor::FlatTupleIterator> iterator;
    std::shared_ptr<processor::FlatTuple> tuple;

    // execution statistics
    std::unique_ptr<QuerySummary> querySummary;
};

} // namespace main
} // namespace kuzu
