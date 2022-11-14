#include "include/query_result.h"

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

void QueryResult::writeToCSV(string fileName) {
    ofstream file;
    file.open(fileName);
    shared_ptr<FlatTuple> nextTuple;
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
            string thisValueStr;
            for (long unsigned int j = 0; j < resultVal.length(); j++) {
                if (!surroundQuotes) {
                    if (resultVal[j] == '"' || resultVal[j] == '\n' || resultVal[j] == ',') {
                        surroundQuotes = true;
                    }
                }
                if (resultVal[j] == '"') {
                    thisValueStr += "\"\"";
                } else if (resultVal[j] == ',' && isStringList) {
                    thisValueStr += "\"\",\"\"";
                } else if (resultVal[j] == '[' && isStringList) {
                    thisValueStr += "[\"\"";
                } else if (resultVal[j] == ']' && isStringList) {
                    thisValueStr += "\"\"]";
                } else {
                    thisValueStr += resultVal[j];
                }
            }
            if (surroundQuotes) {
                thisValueStr = '"' + thisValueStr + '"';
            }
            file << thisValueStr;
            if (idx < nextTuple->len() - 1) {
                file << ",";
            } else {
                file << endl;
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
