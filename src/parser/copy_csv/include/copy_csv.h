#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "src/parser/include/statement.h"

namespace graphflow {
namespace parser {

using namespace std;

class CopyCSV : public Statement {
public:
    explicit CopyCSV(
        string csvFileName, string tableName, unordered_map<string, string> parsingOptions)
        : Statement{StatementType::COPY_CSV}, csvFileName{move(csvFileName)},
          tableName{move(tableName)}, parsingOptions{move(parsingOptions)} {}

    inline string getCSVFileName() const { return csvFileName; }
    inline string getTableName() const { return tableName; }
    inline unordered_map<string, string> getParsingOptions() const { return parsingOptions; }

private:
    string csvFileName;
    string tableName;
    unordered_map<string, string> parsingOptions;
};

} // namespace parser
} // namespace graphflow
