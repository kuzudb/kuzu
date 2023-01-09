#include "main/query_result.h"

#include <fstream>

#include "binder/expression/node_rel_expression.h"
#include "binder/expression/property_expression.h"

using namespace std;
using namespace kuzu::processor;

namespace kuzu {
namespace main {

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
        auto value = make_unique<Value>(columnType);
        if (columnType.typeID == common::NODE || columnType.typeID == common::REL) {
            for (auto& expression : expressionToCollectPerColumn[i]) {
                assert(expression->expressionType == common::PROPERTY);
                auto property = (PropertyExpression*)expression.get();
                auto propertyValue = make_unique<Value>(property->getDataType());
                valuesToCollect.push_back(propertyValue.get());
                value->addNodeOrRelProperty(property->getPropertyName(), std::move(propertyValue));
            }
        } else {
            assert(expressionToCollectPerColumn[i].size() == 1);
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
            string resultVal = nextTuple->getResultValue(idx)->toString();
            bool isStringList = false;
            if (Types::dataTypeToString(nextTuple->getResultValue(idx)->getDataType()) ==
                "STRING[]") {
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
