#pragma once

#include "create_table_info.h"
#include "parser/statement.h"

namespace kuzu {
namespace parser {

class CreateType final : public Statement {
public:
    CreateType(const std::string& name, const std::string& dataType)
        : Statement{common::StatementType::CREATE_TYPE}, name{name}, dataType{dataType} {}

    std::string getName() const { return name; }

    std::string getDataType() const { return dataType; }

private:
    std::string name;
    std::string dataType;
};

} // namespace parser
} // namespace kuzu
