#pragma once

#include "common/api.h"
#include "common/types/types.h"
#include "kuzu_fwd.h"
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

class QueryResult {
    friend class Connection;

public:
    // Only used when we failed to prepare a query.
    KUZU_API QueryResult();
    explicit QueryResult(const PreparedSummary& preparedSummary);
    KUZU_API ~QueryResult();

    KUZU_API bool isSuccess() const;
    KUZU_API std::string getErrorMessage() const;
    KUZU_API size_t getNumColumns() const;
    KUZU_API std::vector<std::string> getColumnNames();
    KUZU_API std::vector<common::DataType> getColumnDataTypes();
    KUZU_API uint64_t getNumTuples();
    KUZU_API QuerySummary* getQuerySummary() const;
    std::vector<std::unique_ptr<DataTypeInfo>> getColumnTypesInfo();

    KUZU_API bool hasNext();
    KUZU_API std::shared_ptr<processor::FlatTuple> getNext();
    KUZU_API void writeToCSV(const std::string& fileName, char delimiter = ',',
        char escapeCharacter = '"', char newline = '\n');
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
