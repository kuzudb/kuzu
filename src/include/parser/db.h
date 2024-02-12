#pragma once

#include "parser/expression/parsed_expression.h"
#include "parser/statement.h"

namespace kuzu {
namespace parser {

class ExportDB : public Statement {
public:
    explicit ExportDB(std::string filePath)
        : Statement{common::StatementType::EXPORT_DATABASE}, filePath{std::move(filePath)} {}

    inline void setParsingOption(parsing_option_t options) { parsingOptions = std::move(options); }
    inline const parsing_option_t& getParsingOptionsRef() const { return parsingOptions; }
    inline std::string getFilePath() const { return filePath; }

private:
    parsing_option_t parsingOptions;
    std::string filePath;
};

} // namespace parser
} // namespace kuzu
