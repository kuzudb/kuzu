#include "main/query_result.h"

#include "binder/expression/expression.h"
#include "common/arrow/arrow_converter.h"
#include "common/exception/runtime.h"
#include "common/types/value/node.h"
#include "common/types/value/rel.h"
#include "processor/result/factorized_table.h"
#include "processor/result/flat_tuple.h"

using namespace kuzu::common;
using namespace kuzu::processor;

namespace kuzu {
namespace main {

QueryResult::QueryResult() = default;

QueryResult::QueryResult(const PreparedSummary& preparedSummary) {
    querySummary = std::make_unique<QuerySummary>();
    querySummary->setPreparedSummary(preparedSummary);
    nextQueryResult = nullptr;
    queryResultIterator = QueryResultIterator{this};
}

QueryResult::~QueryResult() = default;

bool QueryResult::isSuccess() const {
    return success;
}

std::string QueryResult::getErrorMessage() const {
    return errMsg;
}

size_t QueryResult::getNumColumns() const {
    return columnDataTypes.size();
}

std::vector<std::string> QueryResult::getColumnNames() const {
    return columnNames;
}

std::vector<LogicalType> QueryResult::getColumnDataTypes() const {
    return LogicalType::copy(columnDataTypes);
}

uint64_t QueryResult::getNumTuples() const {
    return factorizedTable->getTotalNumFlatTuples();
}

QuerySummary* QueryResult::getQuerySummary() const {
    return querySummary.get();
}

void QueryResult::resetIterator() {
    iterator->resetState();
}

void QueryResult::initResultTableAndIterator(
    std::shared_ptr<processor::FactorizedTable> factorizedTable_,
    const binder::expression_vector& columns) {
    factorizedTable = std::move(factorizedTable_);
    tuple = std::make_shared<FlatTuple>();
    std::vector<Value*> valuesToCollect;
    for (auto i = 0u; i < columns.size(); ++i) {
        auto column = columns[i].get();
        const auto& columnType = column->getDataType();
        auto columnName = column->hasAlias() ? column->getAlias() : column->toString();
        columnDataTypes.push_back(columnType.copy());
        columnNames.push_back(columnName);
        std::unique_ptr<Value> value =
            std::make_unique<Value>(Value::createDefaultValue(columnType.copy()));
        valuesToCollect.push_back(value.get());
        tuple->addValue(std::move(value));
    }
    iterator = std::make_unique<FlatTupleIterator>(*factorizedTable, std::move(valuesToCollect));
}

bool QueryResult::hasNext() const {
    validateQuerySucceed();
    return iterator->hasNextFlatTuple();
}

bool QueryResult::hasNextQueryResult() const {
    return queryResultIterator.hasNextQueryResult();
}

QueryResult* QueryResult::getNextQueryResult() {
    if (hasNextQueryResult()) {
        ++queryResultIterator;
        return queryResultIterator.getCurrentResult();
    }
    return nullptr;
}

std::shared_ptr<FlatTuple> QueryResult::getNext() {
    if (!hasNext()) {
        throw RuntimeException(
            "No more tuples in QueryResult, Please check hasNext() before calling getNext().");
    }
    validateQuerySucceed();
    iterator->getNextFlatTuple();
    return tuple;
}

std::string QueryResult::toString() {
    std::string result;
    if (isSuccess()) {
        // print header
        for (auto i = 0u; i < columnNames.size(); ++i) {
            if (i != 0) {
                result += "|";
            }
            result += columnNames[i];
        }
        result += "\n";
        resetIterator();
        while (hasNext()) {
            getNext();
            result += tuple->toString();
        }
    } else {
        result = errMsg;
    }
    return result;
}

void QueryResult::validateQuerySucceed() const {
    if (!success) {
        throw Exception(errMsg);
    }
}

std::unique_ptr<ArrowSchema> QueryResult::getArrowSchema() const {
    return ArrowConverter::toArrowSchema(getColumnDataTypes(), getColumnNames());
}

std::unique_ptr<ArrowArray> QueryResult::getNextArrowChunk(int64_t chunkSize) {
    auto data = std::make_unique<ArrowArray>();
    ArrowConverter::toArrowArray(*this, data.get(), chunkSize);
    return data;
}

} // namespace main
} // namespace kuzu
