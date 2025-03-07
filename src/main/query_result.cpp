#include "main/query_result.h"

#include <memory>

#include "common/arrow/arrow_converter.h"
#include "common/exception/runtime.h"
#include "processor/result/factorized_table.h"
#include "processor/result/flat_tuple.h"

using namespace kuzu::common;
using namespace kuzu::processor;

namespace kuzu {
namespace main {

QueryResult::QueryResult() : nextQueryResult{nullptr}, queryResultIterator{this} {}

QueryResult::QueryResult(const PreparedSummary& preparedSummary)
    : nextQueryResult{nullptr}, queryResultIterator{this} {
    querySummary = std::make_unique<QuerySummary>();
    querySummary->setPreparedSummary(preparedSummary);
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

void QueryResult::setColumnHeader(std::vector<std::string> columnNames_,
    std::vector<LogicalType> columnTypes_) {
    columnNames = std::move(columnNames_);
    columnDataTypes = std::move(columnTypes_);
}

std::pair<std::unique_ptr<FlatTuple>, std::unique_ptr<FlatTupleIterator>>
QueryResult::getIterator() const {
    std::vector<Value*> valuesToCollect;
    auto tuple = std::make_unique<FlatTuple>();
    for (auto& type : columnDataTypes) {
        auto value = std::make_unique<Value>(Value::createDefaultValue(type.copy()));
        valuesToCollect.push_back(value.get());
        tuple->addValue(std::move(value));
    }
    return std::make_pair(std::move(tuple),
        std::make_unique<FlatTupleIterator>(*factorizedTable, std::move(valuesToCollect)));
}

void QueryResult::initResultTableAndIterator(
    std::shared_ptr<processor::FactorizedTable> factorizedTable_) {
    factorizedTable = std::move(factorizedTable_);
    auto [tuple, iterator] = getIterator();
    this->iterator = std::move(iterator);
    this->tuple = std::move(tuple);
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

std::string QueryResult::toString() const {
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
        auto [tuple, iterator] = getIterator();
        while (iterator->hasNextFlatTuple()) {
            iterator->getNextFlatTuple();
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
