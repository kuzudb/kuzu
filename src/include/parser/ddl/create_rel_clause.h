#pragma once

#include "parser/ddl/create_table.h"

namespace kuzu {
namespace parser {

using namespace std;

class CreateRelClause : public CreateTable {
public:
    explicit CreateRelClause(string tableName, vector<pair<string, string>> propertyNameDataTypes,
        string relMultiplicity, vector<pair<string, string>> relConnections)
        : CreateTable{StatementType::CREATE_REL_CLAUSE, move(tableName),
              move(propertyNameDataTypes)},
          relMultiplicity{move(relMultiplicity)}, relConnections{move(relConnections)} {}

    inline string getRelMultiplicity() const { return relMultiplicity; }

    inline vector<pair<string, string>> getRelConnections() const { return relConnections; }

private:
    string relMultiplicity;
    vector<pair<string, string>> relConnections;
};

} // namespace parser
} // namespace kuzu
