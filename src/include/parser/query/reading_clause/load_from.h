#pragma once

#include "parser/expression/parsed_expression.h"
#include "reading_clause.h"

namespace kuzu {
namespace parser {

class LoadFrom : public ReadingClause {
public:
    LoadFrom(std::vector<std::string> filePaths, parsing_option_t parsingOptions)
        : ReadingClause{common::ClauseType::LOAD_FROM}, filePaths{std::move(filePaths)},
          parsingOptions{std::move(parsingOptions)} {}

    inline std::vector<std::string> getFilePaths() const { return filePaths; }
    inline const parsing_option_t& getParsingOptionsRef() const { return parsingOptions; }

    inline void setWherePredicate(std::unique_ptr<ParsedExpression> expression) {
        wherePredicate = std::move(expression);
    }
    inline bool hasWherePredicate() const { return wherePredicate != nullptr; }
    inline ParsedExpression* getWherePredicate() const { return wherePredicate.get(); }

private:
    std::vector<std::string> filePaths;
    parsing_option_t parsingOptions;
    std::unique_ptr<ParsedExpression> wherePredicate;
};

} // namespace parser
} // namespace kuzu
