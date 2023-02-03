#include "main/query_result.h"

#include <fstream>

#include "binder/expression/node_rel_expression.h"
#include "binder/expression/property_expression.h"

using namespace kuzu::processor;

namespace kuzu {
namespace main {

std::unique_ptr<DataTypeInfo> DataTypeInfo::getInfoForDataType(
    const DataType& type, const string& name) {
    auto columnTypeInfo = make_unique<DataTypeInfo>(type.typeID, name);
    switch (type.typeID) {
    case common::INTERNAL_ID: {
        columnTypeInfo->childrenTypesInfo.push_back(
            make_unique<DataTypeInfo>(common::INT64, "offset"));
        columnTypeInfo->childrenTypesInfo.push_back(
            make_unique<DataTypeInfo>(common::INT64, "tableID"));
    } break;
    case common::LIST: {
        auto parentTypeInfo = columnTypeInfo.get();
        auto childType = type.childType.get();
        parentTypeInfo->childrenTypesInfo.push_back(getInfoForDataType(*childType, ""));
    } break;
    default: {
        // DO NOTHING
    }
    }
    return std::move(columnTypeInfo);
}

vector<unique_ptr<DataTypeInfo>> QueryResult::getColumnTypesInfo() {
    vector<unique_ptr<DataTypeInfo>> result;
    for (auto i = 0u; i < columnDataTypes.size(); i++) {
        auto columnTypeInfo = DataTypeInfo::getInfoForDataType(columnDataTypes[i], columnNames[i]);
        if (columnTypeInfo->typeID == common::NODE) {
            auto value = tuple->getValue(i)->nodeVal.get();
            columnTypeInfo->childrenTypesInfo.push_back(
                DataTypeInfo::getInfoForDataType(DataType(common::INTERNAL_ID), "_id"));
            columnTypeInfo->childrenTypesInfo.push_back(
                DataTypeInfo::getInfoForDataType(DataType(common::STRING), "_label"));
            for (auto& [name, val] : value->getProperties()) {
                columnTypeInfo->childrenTypesInfo.push_back(
                    DataTypeInfo::getInfoForDataType(val->dataType, name));
            }
        } else if (columnTypeInfo->typeID == common::REL) {
            auto value = tuple->getValue(i)->relVal.get();
            columnTypeInfo->childrenTypesInfo.push_back(
                DataTypeInfo::getInfoForDataType(DataType(common::INTERNAL_ID), "_src"));
            columnTypeInfo->childrenTypesInfo.push_back(
                DataTypeInfo::getInfoForDataType(DataType(common::INTERNAL_ID), "_dst"));
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
    std::shared_ptr<processor::FactorizedTable> factorizedTable_, const expression_vector& columns,
    const vector<expression_vector>& expressionToCollectPerColumn) {
    factorizedTable = std::move(factorizedTable_);
    tuple = make_shared<FlatTuple>();
    vector<Value*> valuesToCollect;
    for (auto i = 0u; i < columns.size(); ++i) {
        auto column = columns[i].get();
        auto columnType = column->getDataType();
        auto columnName = column->hasAlias() ? column->getAlias() : column->getRawName();
        columnDataTypes.push_back(columnType);
        columnNames.push_back(columnName);
        auto expressionsToCollect = expressionToCollectPerColumn[i];
        unique_ptr<Value> value;
        if (columnType.typeID == common::NODE) {
            // first expression is node ID.
            assert(expressionsToCollect[0]->dataType.typeID == common::INTERNAL_ID);
            auto nodeIDVal = make_unique<Value>(Value::createDefaultValue(DataType(INTERNAL_ID)));
            valuesToCollect.push_back(nodeIDVal.get());
            // second expression is node label function.
            assert(expressionsToCollect[1]->dataType.typeID == common::STRING);
            auto labelNameVal = make_unique<Value>(Value::createDefaultValue(DataType(STRING)));
            valuesToCollect.push_back(labelNameVal.get());
            auto nodeVal = make_unique<NodeVal>(std::move(nodeIDVal), std::move(labelNameVal));
            for (auto j = 2u; j < expressionsToCollect.size(); ++j) {
                assert(expressionsToCollect[j]->expressionType == common::PROPERTY);
                auto property = (PropertyExpression*)expressionsToCollect[j].get();
                auto propertyValue =
                    make_unique<Value>(Value::createDefaultValue(property->getDataType()));
                valuesToCollect.push_back(propertyValue.get());
                nodeVal->addProperty(property->getPropertyName(), std::move(propertyValue));
            }
            value = make_unique<Value>(std::move(nodeVal));
        } else if (columnType.typeID == common::REL) {
            // first expression is src node ID.
            assert(expressionsToCollect[0]->dataType.typeID == common::INTERNAL_ID);
            auto srcNodeIDVal =
                make_unique<Value>(Value::createDefaultValue(DataType(INTERNAL_ID)));
            valuesToCollect.push_back(srcNodeIDVal.get());
            // second expression is dst node ID.
            assert(expressionsToCollect[1]->dataType.typeID == common::INTERNAL_ID);
            auto dstNodeIDVal =
                make_unique<Value>(Value::createDefaultValue(DataType(INTERNAL_ID)));
            valuesToCollect.push_back(dstNodeIDVal.get());
            // third expression is rel label function.
            auto labelNameVal = make_unique<Value>(Value::createDefaultValue(DataType(STRING)));
            valuesToCollect.push_back(labelNameVal.get());
            auto relVal = make_unique<RelVal>(
                std::move(srcNodeIDVal), std::move(dstNodeIDVal), std::move(labelNameVal));
            for (auto j = 3u; j < expressionsToCollect.size(); ++j) {
                assert(expressionsToCollect[j]->expressionType == common::PROPERTY);
                auto property = (PropertyExpression*)expressionsToCollect[j].get();
                auto propertyValue =
                    make_unique<Value>(Value::createDefaultValue(property->getDataType()));
                valuesToCollect.push_back(propertyValue.get());
                relVal->addProperty(property->getPropertyName(), std::move(propertyValue));
            }
            value = make_unique<Value>(std::move(relVal));
        } else {
            value = make_unique<Value>(Value::createDefaultValue(columnType));
            assert(expressionsToCollect.size() == 1);
            valuesToCollect.push_back(value.get());
        }
        tuple->addValue(std::move(value));
    }
    iterator = make_unique<FlatTupleIterator>(*factorizedTable, std::move(valuesToCollect));
}

bool QueryResult::hasNext() {
    validateQuerySucceed();
    assert(querySummary->getIsExplain() == false);
    return iterator->hasNextFlatTuple();
}

shared_ptr<FlatTuple> QueryResult::getNext() {
    if (!hasNext()) {
        throw RuntimeException(
            "No more tuples in QueryResult, Please check hasNext() before calling getNext().");
    }
    validateQuerySucceed();
    iterator->getNextFlatTuple();
    return tuple;
}

void QueryResult::writeToCSV(
    const string& fileName, char delimiter, char escapeCharacter, char newline) {
    ofstream file;
    file.open(fileName);
    shared_ptr<FlatTuple> nextTuple;
    assert(delimiter != '\0');
    assert(newline != '\0');
    while (hasNext()) {
        nextTuple = getNext();
        for (auto idx = 0ul; idx < nextTuple->len(); idx++) {
            string resultVal = nextTuple->getValue(idx)->toString();
            bool isStringList = false;
            if (Types::dataTypeToString(nextTuple->getValue(idx)->getDataType()) == "STRING[]") {
                isStringList = true;
            }
            bool surroundQuotes = false;
            string csvStr;
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

void QueryResult::validateQuerySucceed() {
    if (!success) {
        throw Exception(errMsg);
    }
}

} // namespace main
} // namespace kuzu
