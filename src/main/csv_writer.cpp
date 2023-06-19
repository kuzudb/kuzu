#include "main/csv_writer.h"

#include <fstream>

#include "main/query_result.h"

using namespace kuzu::common;

namespace kuzu {
namespace main {

void CSVWriter::writeToCSV(QueryResult& queryResult, const std::string& fileName, char delimiter,
    char escapeCharacter, char newline) {
    std::ofstream file;
    file.open(fileName);
    std::shared_ptr<processor::FlatTuple> nextTuple;
    assert(delimiter != '\0');
    assert(newline != '\0');
    while (queryResult.hasNext()) {
        nextTuple = queryResult.getNext();
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

} // namespace main
} // namespace kuzu
