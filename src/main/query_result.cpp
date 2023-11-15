#include "main/query_result.h"

#include <fstream>

#include "binder/expression/expression.h"
#include "common/arrow/arrow_converter.h"
#include "common/types/value/node.h"
#include "common/types/value/rel.h"
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
    case LogicalTypeID::INTERNAL_ID: {
        columnTypeInfo->childrenTypesInfo.push_back(
            std::make_unique<DataTypeInfo>(LogicalTypeID::INT64, "offset"));
        columnTypeInfo->childrenTypesInfo.push_back(
            std::make_unique<DataTypeInfo>(LogicalTypeID::INT64, "tableID"));
    } break;
    case LogicalTypeID::VAR_LIST: {
        auto parentTypeInfo = columnTypeInfo.get();
        auto childType = VarListType::getChildType(&type);
        parentTypeInfo->childrenTypesInfo.push_back(getInfoForDataType(*childType, ""));
    } break;
    case LogicalTypeID::FIXED_LIST: {
        auto parentTypeInfo = columnTypeInfo.get();
        parentTypeInfo->numValuesPerList = FixedListType::getNumValuesInList(&type);
        auto childType = FixedListType::getChildType(&type);
        parentTypeInfo->childrenTypesInfo.push_back(getInfoForDataType(*childType, ""));
    } break;
    case LogicalTypeID::STRUCT: {
        auto parentTypeInfo = columnTypeInfo.get();
        for (auto i = 0u; i < StructType::getNumFields(&type); i++) {
            auto structField = StructType::getField(&type, i);
            parentTypeInfo->childrenTypesInfo.push_back(
                getInfoForDataType(*structField->getType(), structField->getName()));
        }
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

std::vector<LogicalType> QueryResult::getColumnDataTypes() const {
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

std::vector<std::unique_ptr<DataTypeInfo>> QueryResult::getColumnTypesInfo() const {
    std::vector<std::unique_ptr<DataTypeInfo>> result;
    for (auto i = 0u; i < columnDataTypes.size(); i++) {
        auto columnTypeInfo = DataTypeInfo::getInfoForDataType(columnDataTypes[i], columnNames[i]);
        if (columnTypeInfo->typeID == LogicalTypeID::NODE) {
            auto value = tuple->getValue(i);
            columnTypeInfo->childrenTypesInfo.push_back(
                DataTypeInfo::getInfoForDataType(LogicalType(LogicalTypeID::INTERNAL_ID), "_id"));
            columnTypeInfo->childrenTypesInfo.push_back(
                DataTypeInfo::getInfoForDataType(LogicalType(LogicalTypeID::STRING), "_label"));
            auto numProperties = NodeVal::getNumProperties(value);
            for (auto j = 0u; j < numProperties; j++) {
                auto name = NodeVal::getPropertyName(value, j);
                auto val = NodeVal::getPropertyVal(value, j);
                columnTypeInfo->childrenTypesInfo.push_back(
                    DataTypeInfo::getInfoForDataType(*val->getDataType(), name));
            }
        } else if (columnTypeInfo->typeID == LogicalTypeID::REL) {
            auto value = tuple->getValue(i);
            columnTypeInfo->childrenTypesInfo.push_back(
                DataTypeInfo::getInfoForDataType(LogicalType(LogicalTypeID::INTERNAL_ID), "_src"));
            columnTypeInfo->childrenTypesInfo.push_back(
                DataTypeInfo::getInfoForDataType(LogicalType(LogicalTypeID::INTERNAL_ID), "_dst"));
            auto numProperties = RelVal::getNumProperties(value);
            for (auto j = 0u; j < numProperties; j++) {
                auto name = RelVal::getPropertyName(value, j);
                auto val = RelVal::getPropertyVal(value, j);
                columnTypeInfo->childrenTypesInfo.push_back(
                    DataTypeInfo::getInfoForDataType(*val->getDataType(), name));
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
    KU_ASSERT(delimiter != '\0');
    KU_ASSERT(newline != '\0');
    while (hasNext()) {
        nextTuple = getNext();
        for (auto idx = 0ul; idx < nextTuple->len(); idx++) {
            std::string resultVal = nextTuple->getValue(idx)->toString();
            bool isStringList = false;
            if (nextTuple->getValue(idx)->getDataType()->toString() == "STRING[]") {
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

std::unique_ptr<ArrowSchema> QueryResult::getArrowSchema() const {
    return ArrowConverter::toArrowSchema(getColumnTypesInfo());
}

std::unique_ptr<ArrowArray> QueryResult::getNextArrowChunk(int64_t chunkSize) {
    auto data = std::make_unique<ArrowArray>();
    ArrowConverter::toArrowArray(*this, data.get(), chunkSize);
    return data;
}

} // namespace main
} // namespace kuzu
