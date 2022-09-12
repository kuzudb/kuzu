#pragma once

#include <vector>

#include "src/parser/ddl/include/ddl.h"

namespace graphflow {
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
} // namespace graphflow
