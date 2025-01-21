#pragma once

#include <vector>

#include "parser/expression/parsed_expression.h"
#include "parser/scan_source.h"
#include "parser/statement.h"

namespace kuzu {
namespace parser {

class Copy : public Statement {
public:
    explicit Copy(common::StatementType type) : Statement{type} {}

    void setParsingOption(options_t options) { parsingOptions = std::move(options); }
    const options_t& getParsingOptions() const { return parsingOptions; }

protected:
    options_t parsingOptions;
};

class CopyFrom : public Copy {
public:
    CopyFrom(std::unique_ptr<BaseScanSource> source, std::string tableName)
        : Copy{common::StatementType::COPY_FROM}, byColumn_{false}, source{std::move(source)},
          tableName{std::move(tableName)} {}

    void setByColumn() { byColumn_ = true; }
    bool byColumn() const { return byColumn_; }

    BaseScanSource* getSource() const { return source.get(); }

    std::string getTableName() const { return tableName; }

    void setColumnNames(std::vector<std::string> names) { columnNames = std::move(names); }
    std::vector<std::string> getColumnNames() const { return columnNames; }

private:
    bool byColumn_;
    std::unique_ptr<BaseScanSource> source;
    std::string tableName;
    std::vector<std::string> columnNames;
};

class CopyTo : public Copy {
public:
    CopyTo(std::string filePath, std::unique_ptr<Statement> statement)
        : Copy{common::StatementType::COPY_TO}, filePath{std::move(filePath)},
          statement{std::move(statement)} {}

    std::string getFilePath() const { return filePath; }
    const Statement* getStatement() const { return statement.get(); }

private:
    std::string filePath;
    std::unique_ptr<Statement> statement;
};

} // namespace parser
} // namespace kuzu
