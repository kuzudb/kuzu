#pragma once

#include "common/types/types.h"
#include "parser/ddl/ddl.h"
#include "parser/expression/parsed_expression.h"

namespace kuzu {
namespace parser {

class AddProperty : public DDL {
public:
    explicit AddProperty(std::string tableName, std::string propertyName, std::string dataType,
        std::unique_ptr<ParsedExpression> defaultValue)
        : DDL{common::StatementType::ADD_PROPERTY, std::move(tableName)}, propertyName{std::move(
                                                                              propertyName)},
          dataType{std::move(dataType)}, defaultValue{std::move(defaultValue)} {}

    inline std::string getPropertyName() const { return propertyName; }

    inline std::string getDataType() const { return dataType; }

    inline ParsedExpression* getDefaultValue() const { return defaultValue.get(); }

private:
    std::string propertyName;
    std::string dataType;
    std::unique_ptr<ParsedExpression> defaultValue;
};

} // namespace parser
} // namespace kuzu
