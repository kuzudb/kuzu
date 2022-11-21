#pragma once

#include "common/types/types.h"
#include "processor/result/factorized_table.h"
#include "processor/result/flat_tuple.h"
#include "query_summary.h"

using namespace kuzu::processor;

namespace kuzu {
namespace main {

struct QueryResultHeader {

    explicit QueryResultHeader(expression_vector expressions);

    QueryResultHeader(
        std::vector<common::DataType> columnDataTypes, std::vector<std::string> columnNames)
        : columnDataTypes{move(columnDataTypes)}, columnNames{move(columnNames)} {}

    unique_ptr<QueryResultHeader> copy() const {
        return make_unique<QueryResultHeader>(columnDataTypes, columnNames);
    }

    std::vector<common::DataType> columnDataTypes;
    std::vector<std::string> columnNames;
};

class QueryResult {
    friend class Connection;

public:
    // Only used when we failed to prepare a query.
    QueryResult() = default;
    explicit QueryResult(PreparedSummary preparedSummary) {
        querySummary = make_unique<QuerySummary>();
        querySummary->setPreparedSummary(preparedSummary);
    }

    inline bool isSuccess() const { return success; }
    inline string getErrorMessage() const { return errMsg; }

    inline void setResultHeaderAndTable(std::unique_ptr<QueryResultHeader> header,
        std::shared_ptr<processor::FactorizedTable> factorizedTable) {
        this->header = move(header);
        this->factorizedTable = move(factorizedTable);
        resetIterator();
    }

    bool hasNext();

    // TODO: this is not efficient and should be replaced by iterator
    std::shared_ptr<processor::FlatTuple> getNext();

    void writeToCSV(
        string fileName, char delimiter = ',', char escapeCharacter = '"', char newline = '\n');

    inline uint64_t getNumColumns() const { return header->columnDataTypes.size(); }

    inline QuerySummary* getQuerySummary() const { return querySummary.get(); }

    // TODO: interfaces below should be removed
    // used in shell to walk the result twice (first time getting maximum column width)
    inline void resetIterator() {
        iterator = make_unique<FlatTupleIterator>(*factorizedTable, header->columnDataTypes);
    }

    inline uint64_t getNumTuples() {
        return querySummary->getIsExplain() ? 0 : factorizedTable->getTotalNumFlatTuples();
    }

    inline vector<std::string> getColumnNames() { return header->columnNames; }

    inline std::vector<common::DataType> getColumnDataTypes() { return header->columnDataTypes; }

private:
    void validateQuerySucceed();

    bool success = true;
    std::string errMsg;

    std::unique_ptr<QueryResultHeader> header;
    std::shared_ptr<processor::FactorizedTable> factorizedTable;
    std::unique_ptr<processor::FlatTupleIterator> iterator;
    std::unique_ptr<QuerySummary> querySummary;
};

} // namespace main
} // namespace kuzu
