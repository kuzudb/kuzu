#pragma once

#include "parser/statement.h"

namespace kuzu {
namespace parser {

class UpdateVectorIndex final : public Statement {
public:
    UpdateVectorIndex(std::string tableName, std::string propertyName)
        : Statement{common::StatementType::UPDATE_VECTOR_INDEX}, tableName{std::move(tableName)},
          propertyName{std::move(propertyName)} {}

    inline std::string getTableName() const { return tableName; }
    inline std::string getPropertyName() const { return propertyName; }

private:
    std::string tableName;
    std::string propertyName;
};

} // namespace parser
} // namespace kuzu
