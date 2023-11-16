#pragma once

#include "parser/expression/parsed_expression.h"
#include "parser/query/regular_query.h"
#include "parser/statement.h"

namespace kuzu {
namespace parser {

class Copy : public Statement {
public:
    explicit Copy(common::StatementType type, parsing_option_t parsingOptions)
        : Statement{type}, parsingOptions{std::move(parsingOptions)} {}

    inline const parsing_option_t& getParsingOptionsRef() const { return parsingOptions; }

protected:
    parsing_option_t parsingOptions;
};

class CopyFrom : public Copy {
public:
    explicit CopyFrom(bool byColumn_, std::vector<std::string> filePaths, std::string tableName,
        parsing_option_t parsingOptions)
        : Copy{common::StatementType::COPY_FROM, std::move(parsingOptions)}, byColumn_{byColumn_},
          filePaths{std::move(filePaths)}, tableName{std::move(tableName)} {}

    inline bool byColumn() const { return byColumn_; }
    inline std::vector<std::string> getFilePaths() const { return filePaths; }
    inline std::string getTableName() const { return tableName; }

private:
    bool byColumn_;
    std::vector<std::string> filePaths;
    std::string tableName;
};

class CopyTo : public Copy {
public:
    explicit CopyTo(std::string filePath, std::unique_ptr<RegularQuery> regularQuery,
        parsing_option_t parsingOptions)
        : Copy{common::StatementType::COPY_TO, std::move(parsingOptions)},
          regularQuery{std::move(regularQuery)}, filePath{std::move(filePath)} {}

    inline std::string getFilePath() const { return filePath; }
    inline RegularQuery* getRegularQuery() const { return regularQuery.get(); }

private:
    std::string filePath;
    std::unique_ptr<RegularQuery> regularQuery;
};

} // namespace parser
} // namespace kuzu
