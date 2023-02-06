#pragma once

#include "common/types/types.h"
#include "processor/result/factorized_table.h"
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

class QueryResult {
    friend class Connection;

public:
    // Only used when we failed to prepare a query.
    QueryResult() = default;
    explicit QueryResult(const PreparedSummary& preparedSummary) {
        querySummary = std::make_unique<QuerySummary>();
        querySummary->setPreparedSummary(preparedSummary);
    }

    inline bool isSuccess() const { return success; }
    inline std::string getErrorMessage() const { return errMsg; }

    inline size_t getNumColumns() const { return columnDataTypes.size(); }
    inline std::vector<std::string> getColumnNames() { return columnNames; }
    inline std::vector<common::DataType> getColumnDataTypes() { return columnDataTypes; }

    inline uint64_t getNumTuples() {
        return querySummary->getIsExplain() ? 0 : factorizedTable->getTotalNumFlatTuples();
    }

    inline QuerySummary* getQuerySummary() const { return querySummary.get(); }

    std::vector<std::unique_ptr<DataTypeInfo>> getColumnTypesInfo();

    void initResultTableAndIterator(std::shared_ptr<processor::FactorizedTable> factorizedTable_,
        const binder::expression_vector& columns,
        const std::vector<binder::expression_vector>& expressionToCollectPerColumn);

    bool hasNext();

    std::shared_ptr<processor::FlatTuple> getNext();

    void writeToCSV(const std::string& fileName, char delimiter = ',', char escapeCharacter = '"',
        char newline = '\n');

    // TODO: interfaces below should be removed
    // used in shell to walk the result twice (first time getting maximum column width)
    inline void resetIterator() { iterator->resetState(); }

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
    std::shared_ptr<processor::FlatTuple> tuple;

    // execution statistics
    std::unique_ptr<QuerySummary> querySummary;
};

} // namespace main
} // namespace kuzu
