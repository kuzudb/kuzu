#pragma once

#include <string>
#include <unordered_map>

#include "common/copier_config/copier_config.h"
#include "parser/expression/parsed_expression.h"
#include "parser/query/regular_query.h"
#include "parser/statement.h"

namespace kuzu {
namespace parser {

class CopyFrom : public Statement {
public:
    explicit CopyFrom(std::vector<std::string> filePaths, std::string tableName,
        std::unordered_map<std::string, std::unique_ptr<ParsedExpression>> parsingOptions,
        common::CopyDescription::FileType fileType)
        : Statement{common::StatementType::COPY_FROM}, filePaths{std::move(filePaths)},
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

class CopyTo : public Statement {
public:
    explicit CopyTo(std::string filePath, std::unique_ptr<RegularQuery> regularQuery)
        : Statement{common::StatementType::COPY_TO},
          regularQuery{std::move(regularQuery)}, filePath{std::move(filePath)} {}

    inline std::string getFilePath() const { return filePath; }
    inline std::unique_ptr<RegularQuery> getRegularQuery() { return std::move(regularQuery); }

private:
    std::string filePath;
    std::unique_ptr<RegularQuery> regularQuery;
};

} // namespace parser
} // namespace kuzu
