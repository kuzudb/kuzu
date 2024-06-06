#pragma once

#include "parser/statement.h"

namespace kuzu {
namespace parser {

class CreateType final : public Statement {
    static constexpr common::StatementType type_ = common::StatementType::CREATE_TYPE;

public:
    CreateType(const std::string& name, const std::string& dataType)
        : Statement{type_}, name{name}, dataType{dataType} {}

    std::string getName() const { return name; }

    std::string getDataType() const { return dataType; }

private:
    std::string name;
    std::string dataType;
};

} // namespace parser
} // namespace kuzu
