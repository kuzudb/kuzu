#pragma once

#include "parser/expression/parsed_expression.h"
#include "parser/statement.h"

namespace kuzu {
namespace parser {

class CreateVectorIndex final : public Statement {
public:
    CreateVectorIndex(std::string tableName, std::string propertyName, options_t parsingOptions)
        : Statement{common::StatementType::CREATE_VECTOR_INDEX}, tableName{std::move(tableName)},
          propertyName{std::move(propertyName)}, parsingOptions{std::move(parsingOptions)} {}

    inline std::string getTableName() const { return tableName; }
    inline std::string getPropertyName() const { return propertyName; }
    inline const options_t& getParsingOptionsRef() const { return parsingOptions; }

private:
    std::string tableName;
    std::string propertyName;
    options_t parsingOptions;
};

} // namespace parser
} // namespace kuzu
