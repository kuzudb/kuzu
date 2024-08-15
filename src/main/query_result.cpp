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

size_t QueryResult::getNumWarningColumns() const {
    return 4;
}

std::vector<std::string> QueryResult::getColumnNames() const {
    return columnNames;
}

std::vector<std::string> QueryResult::getWarningColumnNames() const {
    return {"Message", "File Path", "Line Number", "Reconstructed Line"};
}

std::vector<LogicalType> QueryResult::getColumnDataTypes() const {
    return LogicalType::copy(columnDataTypes);
}

std::vector<LogicalType> QueryResult::getWarningColumnDataTypes() const {
    std::vector<LogicalType> types;
    types.push_back(LogicalType::STRING());
    types.push_back(LogicalType::STRING());
    types.push_back(LogicalType::UINT64());
    types.push_back(LogicalType::STRING());
    return LogicalType::copy(types);
}

uint64_t QueryResult::getNumTuples() const {
    return factorizedTable->getTotalNumFlatTuples();
}

uint64_t QueryResult::getNumWarnings() const {
    return warningTable->getTotalNumFlatTuples();
}

QuerySummary* QueryResult::getQuerySummary() const {
    return querySummary.get();
}

void QueryResult::resetIterator() {
    iterator->resetState();
}

void QueryResult::resetWarningIterator() {
    warningIterator->resetState();
}

void QueryResult::initResultTableAndIterator(
    std::shared_ptr<processor::FactorizedTable> factorizedTable_,
    std::shared_ptr<processor::FactorizedTable> warningTable_,
    const binder::expression_vector& columns) {
    factorizedTable = std::move(factorizedTable_);
    warningTable = std::move(warningTable_);
    tuple = std::make_shared<FlatTuple>();
    warning = std::make_shared<FlatTuple>();
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

    std::vector<Value*> warningsToCollect;
    const auto warningTypes = {LogicalType::STRING(), LogicalType::STRING(), LogicalType::UINT64(),
        LogicalType::STRING()};
    for (auto& warningType : warningTypes) {
        std::unique_ptr<Value> value =
            std::make_unique<Value>(Value::createDefaultValue(warningType.copy()));
        warningsToCollect.push_back(value.get());
        warning->addValue(std::move(value));
    }

    iterator = std::make_unique<FlatTupleIterator>(*factorizedTable, std::move(valuesToCollect));
    warningIterator =
        std::make_unique<FlatTupleIterator>(*warningTable, std::move(warningsToCollect));
}

bool QueryResult::hasNext() const {
    validateQuerySucceed();
    return iterator->hasNextFlatTuple();
}

bool QueryResult::hasNextWarning() const {
    validateQuerySucceed();
    return warningIterator->hasNextFlatTuple();
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

std::shared_ptr<FlatTuple> QueryResult::getNextWarning() {
    if (!hasNextWarning()) {
        throw RuntimeException("No more warnings in QueryResult, Please check hasNextWarning() "
                               "before calling getNextWarning().");
    }
    validateQuerySucceed();
    warningIterator->getNextFlatTuple();
    return warning;
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

std::string QueryResult::getWarningString() {
    std::string result;
    if (isSuccess()) {
        // print header
        const std::vector<std::string> columnNames{
            {"Message", "File Path", "Line Number", "Reconstructed Line"}};
        for (auto i = 0u; i < columnNames.size(); ++i) {
            if (i != 0) {
                result += "|";
            }
            result += columnNames[i];
        }
        result += "\n";
        resetWarningIterator();
        while (hasNextWarning()) {
            getNextWarning();
            result += warning->toString();
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
