#pragma once

#include <vector>

#include "parser/ddl/ddl.h"

namespace kuzu {
namespace parser {

using namespace std;

class CreateTable : public DDL {
public:
    explicit CreateTable(StatementType statementType, string tableName,
        vector<pair<string, string>> propertyNameDataTypes)
        : DDL{statementType, move(tableName)}, propertyNameDataTypes{move(propertyNameDataTypes)} {}

    inline vector<pair<string, string>> getPropertyNameDataTypes() const {
        return propertyNameDataTypes;
    }

protected:
    vector<pair<string, string>> propertyNameDataTypes;
};

} // namespace parser
} // namespace kuzu
