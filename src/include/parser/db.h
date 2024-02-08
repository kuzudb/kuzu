#pragma once

#include <vector>

#include "parser/expression/parsed_expression.h"
#include "parser/statement.h"

namespace kuzu {
namespace parser {

class DB : public Statement {
public:
    explicit DB(common::StatementType type) : Statement{type} {}

    inline void setParsingOption(parsing_option_t options) { parsingOptions = std::move(options); }
    inline const parsing_option_t& getParsingOptionsRef() const { return parsingOptions; }

protected:
    parsing_option_t parsingOptions;
};

class ExportDB : public DB {
public:
    ExportDB(std::string filePath)
        : DB{common::StatementType::EXPORT_DATABASE}, filePath{std::move(filePath)} {}

    inline std::string getFilePath() const { return filePath; }

private:
    std::string filePath;
};

} // namespace parser
} // namespace kuzu
