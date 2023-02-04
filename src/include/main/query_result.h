#pragma once

#include "../common/types/types.h"
#include "forward_declarations.h"
#include "query_summary.h"

using namespace kuzu::processor;

namespace kuzu {
namespace main {

struct DataTypeInfo {
public:
    DataTypeInfo(DataTypeID typeID, std::string name) : typeID{typeID}, name{std::move(name)} {}

    DataTypeID typeID;
    std::string name;
    std::vector<std::unique_ptr<DataTypeInfo>> childrenTypesInfo;

    static std::unique_ptr<DataTypeInfo> getInfoForDataType(
        const DataType& type, const string& name);
};

class QueryResult {
    friend class Connection;

public:
    // Only used when we failed to prepare a query.
    QueryResult() = default;
    QueryResult(const PreparedSummary& preparedSummary);

    inline bool isSuccess() const { return success; }
    inline string getErrorMessage() const { return errMsg; }

    inline size_t getNumColumns() const { return columnDataTypes.size(); }
    inline vector<std::string> getColumnNames() { return columnNames; }
    inline std::vector<common::DataType> getColumnDataTypes() { return columnDataTypes; }

    inline QuerySummary* getQuerySummary() const { return querySummary.get(); }

    uint64_t getNumTuples();

    vector<unique_ptr<DataTypeInfo>> getColumnTypesInfo();

    void initResultTableAndIterator(std::shared_ptr<processor::FactorizedTable> factorizedTable_,
        const expression_vector& columns,
        const vector<expression_vector>& expressionToCollectPerColumn);

    bool hasNext();

    std::shared_ptr<processor::FlatTuple> getNext();

    void writeToCSV(const string& fileName, char delimiter = ',', char escapeCharacter = '"',
        char newline = '\n');

    // TODO: interfaces below should be removed
    // used in shell to walk the result twice (first time getting maximum column width)
    void resetIterator();

    ~QueryResult();

private:
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
    shared_ptr<FlatTuple> tuple;

    // execution statistics
    std::unique_ptr<QuerySummary> querySummary;
};

} // namespace main
} // namespace kuzu
