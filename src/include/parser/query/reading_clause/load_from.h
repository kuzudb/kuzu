#pragma once

#include "parser/ddl/create_table_info.h"
#include "parser/expression/parsed_expression.h"
#include "parser/scan_source.h"
#include "reading_clause.h"

namespace kuzu {
namespace parser {

class LoadFrom : public ReadingClause {
public:
    explicit LoadFrom(std::unique_ptr<BaseScanSource> source)
        : ReadingClause{common::ClauseType::LOAD_FROM}, source{std::move(source)} {}

    inline BaseScanSource* getSource() const { return source.get(); }

    inline void setParingOptions(options_t options) { parsingOptions = std::move(options); }
    inline const options_t& getParsingOptionsRef() const { return parsingOptions; }

    inline void setPropertyDefinitions(std::vector<PropertyDefinition> propertyDefns) {
        propertyDefinitions = std::move(propertyDefns);
    }
    inline const std::vector<PropertyDefinition>& getPropertyDefinitionsRef() const {
        return propertyDefinitions;
    }

    inline void setWherePredicate(std::unique_ptr<ParsedExpression> expression) {
        wherePredicate = std::move(expression);
    }
    inline bool hasWherePredicate() const { return wherePredicate != nullptr; }
    inline const ParsedExpression* getWherePredicate() const { return wherePredicate.get(); }

private:
    std::unique_ptr<BaseScanSource> source;
    std::vector<PropertyDefinition> propertyDefinitions;
    options_t parsingOptions;
    std::unique_ptr<ParsedExpression> wherePredicate;
};

} // namespace parser
} // namespace kuzu
