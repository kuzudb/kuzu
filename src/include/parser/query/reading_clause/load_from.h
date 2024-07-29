#pragma once

#include "parser/ddl/create_table_info.h"
#include "parser/expression/parsed_expression.h"
#include "parser/scan_source.h"
#include "reading_clause.h"

namespace kuzu {
namespace parser {

class LoadFrom : public ReadingClause {
    static constexpr common::ClauseType clauseType_ = common::ClauseType::LOAD_FROM;

public:
    explicit LoadFrom(std::unique_ptr<BaseScanSource> source)
        : ReadingClause{clauseType_}, source{std::move(source)} {}

    BaseScanSource* getSource() const { return source.get(); }

    void setParingOptions(options_t options) { parsingOptions = std::move(options); }
    const options_t& getParsingOptions() const { return parsingOptions; }

    void setPropertyDefinitions(std::vector<PropertyDefinition> propertyDefns) {
        propertyDefinitions = std::move(propertyDefns);
    }
    const std::vector<PropertyDefinition>& getPropertyDefinitions() const {
        return propertyDefinitions;
    }

private:
    std::unique_ptr<BaseScanSource> source;
    std::vector<PropertyDefinition> propertyDefinitions;
    options_t parsingOptions;
};

} // namespace parser
} // namespace kuzu
