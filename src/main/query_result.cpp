#include "../include/main/query_result.h"

#include <fstream>

using namespace std;
using namespace kuzu::processor;

namespace kuzu {
namespace main {

QueryResultHeader::QueryResultHeader(expression_vector expressions) {
    for (auto& expression : expressions) {
        columnDataTypes.push_back(expression->getDataType());
        columnNames.push_back(
            expression->hasAlias() ? expression->getAlias() : expression->getRawName());
    }
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
    return iterator->getNextFlatTuple();
}

void QueryResult::writeToCSV(string fileName, char delimiter, char escapeCharacter, char newline) {
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
