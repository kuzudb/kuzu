#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "parser/expression/parsed_expression.h"
#include "parser/statement.h"

namespace kuzu {
namespace parser {

using namespace std;

class CopyCSV : public Statement {
public:
    explicit CopyCSV(string csvFileName, string tableName,
        unordered_map<string, unique_ptr<ParsedExpression>> parsingOptions)
        : Statement{StatementType::COPY_CSV}, csvFileName{move(csvFileName)},
          tableName{move(tableName)}, parsingOptions{move(parsingOptions)} {}

    inline string getCSVFileName() const { return csvFileName; }
    inline string getTableName() const { return tableName; }
    inline unordered_map<string, unique_ptr<ParsedExpression>> const* getParsingOptions() const {
        return &parsingOptions;
    }

private:
    string csvFileName;
    string tableName;
    unordered_map<string, unique_ptr<ParsedExpression>> parsingOptions;
};

} // namespace parser
} // namespace kuzu
