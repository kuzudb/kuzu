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
    return querySummary->getIsExplain() ? 0 : factorizedTable->getTotalNumFlatTuples();
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
            auto value = tuple->getValue(i)->nodeVal.get();
            columnTypeInfo->childrenTypesInfo.push_back(DataTypeInfo::getInfoForDataType(
                LogicalType(common::LogicalTypeID::INTERNAL_ID), "_id"));
            columnTypeInfo->childrenTypesInfo.push_back(DataTypeInfo::getInfoForDataType(
                LogicalType(common::LogicalTypeID::STRING), "_label"));
            for (auto& [name, val] : value->getProperties()) {
                columnTypeInfo->childrenTypesInfo.push_back(
                    DataTypeInfo::getInfoForDataType(val->dataType, name));
            }
        } else if (columnTypeInfo->typeID == common::LogicalTypeID::REL) {
            auto value = tuple->getValue(i)->relVal.get();
            columnTypeInfo->childrenTypesInfo.push_back(DataTypeInfo::getInfoForDataType(
                LogicalType(common::LogicalTypeID::INTERNAL_ID), "_src"));
            columnTypeInfo->childrenTypesInfo.push_back(DataTypeInfo::getInfoForDataType(
                LogicalType(common::LogicalTypeID::INTERNAL_ID), "_dst"));
            for (auto& [name, val] : value->getProperties()) {
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
    const binder::expression_vector& columns,
    const std::vector<binder::expression_vector>& expressionToCollectPerColumn) {
    factorizedTable = std::move(factorizedTable_);
    tuple = std::make_shared<FlatTuple>();
    std::vector<Value*> valuesToCollect;
    for (auto i = 0u; i < columns.size(); ++i) {
        auto column = columns[i].get();
        auto columnType = column->getDataType();
        auto columnName = column->hasAlias() ? column->getAlias() : column->toString();
        columnDataTypes.push_back(columnType);
        columnNames.push_back(columnName);
        auto expressionsToCollect = expressionToCollectPerColumn[i];
        std::unique_ptr<Value> value;
        if (columnType.getLogicalTypeID() == common::LogicalTypeID::NODE) {
            // first expression is node ID.
            assert(expressionsToCollect[0]->dataType.getLogicalTypeID() ==
                   common::LogicalTypeID::INTERNAL_ID);
            auto nodeIDVal = std::make_unique<Value>(
                Value::createDefaultValue(LogicalType(LogicalTypeID::INTERNAL_ID)));
            valuesToCollect.push_back(nodeIDVal.get());
            // second expression is node label function.
            assert(expressionsToCollect[1]->dataType.getLogicalTypeID() ==
                   common::LogicalTypeID::STRING);
            auto labelNameVal = std::make_unique<Value>(
                Value::createDefaultValue(LogicalType(LogicalTypeID::STRING)));
            valuesToCollect.push_back(labelNameVal.get());
            auto nodeVal = std::make_unique<NodeVal>(std::move(nodeIDVal), std::move(labelNameVal));
            for (auto j = 2u; j < expressionsToCollect.size(); ++j) {
                assert(expressionsToCollect[j]->expressionType == common::PROPERTY);
                auto property = (binder::PropertyExpression*)expressionsToCollect[j].get();
                auto propertyValue =
                    std::make_unique<Value>(Value::createDefaultValue(property->getDataType()));
                valuesToCollect.push_back(propertyValue.get());
                nodeVal->addProperty(property->getPropertyName(), std::move(propertyValue));
            }
            value = std::make_unique<Value>(std::move(nodeVal));
        } else if (columnType.getLogicalTypeID() == common::LogicalTypeID::REL) {
            // first expression is src node ID.
            assert(expressionsToCollect[0]->dataType.getLogicalTypeID() ==
                   common::LogicalTypeID::INTERNAL_ID);
            auto srcNodeIDVal = std::make_unique<Value>(
                Value::createDefaultValue(LogicalType(LogicalTypeID::INTERNAL_ID)));
            valuesToCollect.push_back(srcNodeIDVal.get());
            // second expression is dst node ID.
            assert(expressionsToCollect[1]->dataType.getLogicalTypeID() ==
                   common::LogicalTypeID::INTERNAL_ID);
            auto dstNodeIDVal = std::make_unique<Value>(
                Value::createDefaultValue(LogicalType(LogicalTypeID::INTERNAL_ID)));
            valuesToCollect.push_back(dstNodeIDVal.get());
            // third expression is rel label function.
            auto labelNameVal = std::make_unique<Value>(
                Value::createDefaultValue(LogicalType(LogicalTypeID::STRING)));
            valuesToCollect.push_back(labelNameVal.get());
            auto relVal = std::make_unique<RelVal>(
                std::move(srcNodeIDVal), std::move(dstNodeIDVal), std::move(labelNameVal));
            for (auto j = 3u; j < expressionsToCollect.size(); ++j) {
                assert(expressionsToCollect[j]->expressionType == common::PROPERTY);
                auto property = (binder::PropertyExpression*)expressionsToCollect[j].get();
                auto propertyValue =
                    std::make_unique<Value>(Value::createDefaultValue(property->getDataType()));
                valuesToCollect.push_back(propertyValue.get());
                relVal->addProperty(property->getPropertyName(), std::move(propertyValue));
            }
            value = std::make_unique<Value>(std::move(relVal));
        } else {
            value = std::make_unique<Value>(Value::createDefaultValue(columnType));
            assert(expressionsToCollect.size() == 1);
            valuesToCollect.push_back(value.get());
        }
        tuple->addValue(std::move(value));
    }
    iterator = std::make_unique<FlatTupleIterator>(*factorizedTable, std::move(valuesToCollect));
}

bool QueryResult::hasNext() const {
    validateQuerySucceed();
    assert(querySummary->getIsExplain() == false);
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
