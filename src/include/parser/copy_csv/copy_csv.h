#pragma once

#include <string>
#include <unordered_map>

#include "parser/expression/parsed_expression.h"
#include "parser/statement.h"

namespace kuzu {
namespace parser {

class CopyCSV : public Statement {
public:
    explicit CopyCSV(std::string csvFileName, std::string tableName,
        std::unordered_map<std::string, std::unique_ptr<ParsedExpression>> parsingOptions)
        : Statement{common::StatementType::COPY_CSV}, csvFileName{std::move(csvFileName)},
          tableName{std::move(tableName)}, parsingOptions{std::move(parsingOptions)} {}

    inline std::string getCSVFileName() const { return csvFileName; }
    inline std::string getTableName() const { return tableName; }
    inline std::unordered_map<std::string, std::unique_ptr<ParsedExpression>> const*
    getParsingOptions() const {
        return &parsingOptions;
    }

private:
    std::string csvFileName;
    std::string tableName;
    std::unordered_map<std::string, std::unique_ptr<ParsedExpression>> parsingOptions;
};

} // namespace parser
} // namespace kuzu
