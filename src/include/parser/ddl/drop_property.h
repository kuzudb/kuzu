#pragma once

#include "parser/ddl/ddl.h"

namespace kuzu {
namespace parser {

using namespace std;

class DropProperty : public DDL {
public:
    explicit DropProperty(string tableName, string propertyName)
        : DDL{StatementType::DROP_PROPERTY, std::move(tableName)}, propertyName{
                                                                       std::move(propertyName)} {}

    inline string getPropertyName() const { return propertyName; };

private:
    string propertyName;
};

} // namespace parser
} // namespace kuzu
