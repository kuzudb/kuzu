#pragma once

#include <string>
#include <unordered_map>

#include "parser/expression/parsed_expression.h"
#include "parser/statement.h"

namespace kuzu {
namespace parser {

class Copy : public Statement {
public:
    explicit Copy(std::vector<std::string> filePaths, std::string tableName,
        std::unordered_map<std::string, std::unique_ptr<ParsedExpression>> parsingOptions,
        common::CopyDescription::FileType fileType)
        : Statement{common::StatementType::COPY}, filePaths{std::move(filePaths)},
          tableName{std::move(tableName)},
          parsingOptions{std::move(parsingOptions)}, fileType{fileType} {}

    inline std::vector<std::string> getFilePaths() const { return filePaths; }
    inline std::string getTableName() const { return tableName; }
    inline std::unordered_map<std::string, std::unique_ptr<ParsedExpression>> const*
    getParsingOptions() const {
        return &parsingOptions;
    }
    inline common::CopyDescription::FileType getFileType() const { return fileType; }

private:
    std::vector<std::string> filePaths;
    common::CopyDescription::FileType fileType;
    std::string tableName;
    std::unordered_map<std::string, std::unique_ptr<ParsedExpression>> parsingOptions;
};

} // namespace parser
} // namespace kuzu
