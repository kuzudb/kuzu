#pragma once

#include "common/types/types.h"
#include "parser/ddl/ddl.h"
#include "parser/expression/parsed_expression.h"

namespace kuzu {
namespace parser {

using namespace std;

class AddProperty : public DDL {
public:
    explicit AddProperty(string tableName, string propertyName, string dataType,
        unique_ptr<ParsedExpression> defaultValue)
        : DDL{StatementType::ADD_PROPERTY, std::move(tableName)}, propertyName{std::move(
                                                                      propertyName)},
          dataType{std::move(dataType)}, defaultValue{std::move(defaultValue)} {}

    inline string getPropertyName() const { return propertyName; }

    inline string getDataType() const { return dataType; }

    inline ParsedExpression* getDefaultValue() const { return defaultValue.get(); }

private:
    string propertyName;
    string dataType;
    unique_ptr<ParsedExpression> defaultValue;
};

} // namespace parser
} // namespace kuzu
