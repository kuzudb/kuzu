#pragma once

#include <vector>

#include "parser/expression/parsed_expression.h"
#include "parser/query/regular_query.h"
#include "parser/statement.h"

namespace kuzu {
namespace parser {

class Copy : public Statement {
public:
    explicit Copy(common::StatementType type) : Statement{type} {}

    inline void setParsingOption(parsing_option_t options) { parsingOptions = std::move(options); }
    inline const parsing_option_t& getParsingOptionsRef() const { return parsingOptions; }

protected:
    parsing_option_t parsingOptions;
};

class CopyFrom : public Copy {
public:
    CopyFrom(std::vector<std::string> filePaths, std::string tableName)
        : Copy{common::StatementType::COPY_FROM}, byColumn_{false}, filePaths{std::move(filePaths)},
          tableName{std::move(tableName)} {}

    inline void setByColumn() { byColumn_ = true; }
    inline bool byColumn() const { return byColumn_; }

    inline std::vector<std::string> getFilePaths() const { return filePaths; }
    inline std::string getTableName() const { return tableName; }

    inline void setColumnNames(std::vector<std::string> names) { columnNames = std::move(names); }
    inline std::vector<std::string> getColumnNames() const { return columnNames; }

private:
    bool byColumn_;
    std::vector<std::string> filePaths;
    std::string tableName;
    std::vector<std::string> columnNames;
};

class CopyTo : public Copy {
public:
    CopyTo(std::string filePath, std::unique_ptr<RegularQuery> regularQuery)
        : Copy{common::StatementType::COPY_TO},
          regularQuery{std::move(regularQuery)}, filePath{std::move(filePath)} {}

    inline std::string getFilePath() const { return filePath; }
    inline RegularQuery* getRegularQuery() const { return regularQuery.get(); }

private:
    std::string filePath;
    std::unique_ptr<RegularQuery> regularQuery;
};

} // namespace parser
} // namespace kuzu
