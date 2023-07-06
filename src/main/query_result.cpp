#include "main/query_result.h"

#include <fstream>

#include "binder/expression/node_rel_expression.h"
#include "binder/expression/property_expression.h"
#include "json.hpp"
#include "processor/result/factorized_table.h"
#include "processor/result/flat_tuple.h"

using namespace kuzu::common;
using namespace kuzu::processor;

namespace kuzu {
namespace main {

std::unique_ptr<DataTypeInfo> DataTypeInfo::getInfoForDataType(
    const LogicalType& type, const std::string& name) {
    auto columnTypeInfo = std::make_unique<DataTypeInfo>(type.getLogicalTypeID(), name);
    switch (type.getLogicalTypeID()) {
    case common::LogicalTypeID::INTERNAL_ID: {
        columnTypeInfo->childrenTypesInfo.push_back(
            std::make_unique<DataTypeInfo>(common::LogicalTypeID::INT64, "offset"));
        columnTypeInfo->childrenTypesInfo.push_back(
            std::make_unique<DataTypeInfo>(common::LogicalTypeID::INT64, "tableID"));
    } break;
    case common::LogicalTypeID::VAR_LIST: {
        auto parentTypeInfo = columnTypeInfo.get();
        auto childType = VarListType::getChildType(&type);
        parentTypeInfo->childrenTypesInfo.push_back(getInfoForDataType(*childType, ""));
    } break;
    default: {
        // DO NOTHING
    }
    }
    return std::move(columnTypeInfo);
}

QueryResult::QueryResult() = default;

QueryResult::QueryResult(const PreparedSummary& preparedSummary) {
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

std::vector<common::LogicalType> QueryResult::getColumnDataTypes() const {
    return columnDataTypes;
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

std::vector<std::unique_ptr<DataTypeInfo>> QueryResult::getColumnTypesInfo() {
    std::vector<std::unique_ptr<DataTypeInfo>> result;
    for (auto i = 0u; i < columnDataTypes.size(); i++) {
        auto columnTypeInfo = DataTypeInfo::getInfoForDataType(columnDataTypes[i], columnNames[i]);
        if (columnTypeInfo->typeID == common::LogicalTypeID::NODE) {
            auto value = tuple->getValue(i);
            columnTypeInfo->childrenTypesInfo.push_back(DataTypeInfo::getInfoForDataType(
                LogicalType(common::LogicalTypeID::INTERNAL_ID), "_id"));
            columnTypeInfo->childrenTypesInfo.push_back(DataTypeInfo::getInfoForDataType(
                LogicalType(common::LogicalTypeID::STRING), "_label"));
            auto numProperties = NodeVal::getNumProperties(value);
            for (auto j = 0u; i < numProperties; j++) {
                auto name = NodeVal::getPropertyName(value, j);
                auto val = NodeVal::getPropertyValueReference(value, j);
                columnTypeInfo->childrenTypesInfo.push_back(
                    DataTypeInfo::getInfoForDataType(val->dataType, name));
            }
        } else if (columnTypeInfo->typeID == common::LogicalTypeID::REL) {
            auto value = tuple->getValue(i);
            columnTypeInfo->childrenTypesInfo.push_back(DataTypeInfo::getInfoForDataType(
                LogicalType(common::LogicalTypeID::INTERNAL_ID), "_src"));
            columnTypeInfo->childrenTypesInfo.push_back(DataTypeInfo::getInfoForDataType(
                LogicalType(common::LogicalTypeID::INTERNAL_ID), "_dst"));
            auto numProperties = RelVal::getNumProperties(value);
            for (auto j = 0u; i < numProperties; j++) {
                auto name = NodeVal::getPropertyName(value, j);
                auto val = NodeVal::getPropertyValueReference(value, j);
                columnTypeInfo->childrenTypesInfo.push_back(
                    DataTypeInfo::getInfoForDataType(val->dataType, name));
            }
        }
        result.push_back(std::move(columnTypeInfo));
    }
    return std::move(result);
}

void QueryResult::initResultTableAndIterator(
    std::shared_ptr<processor::FactorizedTable> factorizedTable_,
    const binder::expression_vector& columns) {
    factorizedTable = std::move(factorizedTable_);
    tuple = std::make_shared<FlatTuple>();
    std::vector<Value*> valuesToCollect;
    for (auto i = 0u; i < columns.size(); ++i) {
        auto column = columns[i].get();
        auto columnType = column->getDataType();
        auto columnName = column->hasAlias() ? column->getAlias() : column->toString();
        columnDataTypes.push_back(columnType);
        columnNames.push_back(columnName);
        std::unique_ptr<Value> value =
            std::make_unique<Value>(Value::createDefaultValue(columnType));
        valuesToCollect.push_back(value.get());
        tuple->addValue(std::move(value));
    }
    iterator = std::make_unique<FlatTupleIterator>(*factorizedTable, std::move(valuesToCollect));
}

bool QueryResult::hasNext() const {
    validateQuerySucceed();
    return iterator->hasNextFlatTuple();
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

void QueryResult::writeToCSV(
    const std::string& fileName, char delimiter, char escapeCharacter, char newline) {
    std::ofstream file;
    file.open(fileName);
    std::shared_ptr<FlatTuple> nextTuple;
    assert(delimiter != '\0');
    assert(newline != '\0');
    while (hasNext()) {
        nextTuple = getNext();
        for (auto idx = 0ul; idx < nextTuple->len(); idx++) {
            std::string resultVal = nextTuple->getValue(idx)->toString();
            bool isStringList = false;
            if (LogicalTypeUtils::dataTypeToString(nextTuple->getValue(idx)->getDataType()) ==
                "STRING[]") {
                isStringList = true;
            }
            bool surroundQuotes = false;
            std::string csvStr;
            for (long unsigned int j = 0; j < resultVal.length(); j++) {
                if (!surroundQuotes) {
                    if (resultVal[j] == escapeCharacter || resultVal[j] == newline ||
                        resultVal[j] == delimiter) {
                        surroundQuotes = true;
                    }
                }
                if (resultVal[j] == escapeCharacter) {
                    csvStr += escapeCharacter;
                    csvStr += escapeCharacter;
                } else if (resultVal[j] == ',' && isStringList) {
                    csvStr += escapeCharacter;
                    csvStr += escapeCharacter;
                    csvStr += ',';
                    csvStr += escapeCharacter;
                    csvStr += escapeCharacter;
                } else if (resultVal[j] == '[' && isStringList) {
                    csvStr += "[";
                    csvStr += escapeCharacter;
                    csvStr += escapeCharacter;
                } else if (resultVal[j] == ']' && isStringList) {
                    csvStr += escapeCharacter;
                    csvStr += escapeCharacter;
                    csvStr += "]";
                } else {
                    csvStr += resultVal[j];
                }
            }
            if (surroundQuotes) {
                csvStr = escapeCharacter + csvStr + escapeCharacter;
            }
            file << csvStr;
            if (idx < nextTuple->len() - 1) {
                file << delimiter;
            } else {
                file << newline;
            }
        }
    }
    file.close();
}

void QueryResult::validateQuerySucceed() const {
    if (!success) {
        throw Exception(errMsg);
    }
}

} // namespace main
} // namespace kuzu
