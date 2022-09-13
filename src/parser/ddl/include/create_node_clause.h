#pragma once

#include "src/parser/ddl/include/create_table.h"

namespace graphflow {
namespace parser {

using namespace std;

class CreateNodeClause : public CreateTable {
public:
    explicit CreateNodeClause(
        string tableName, vector<pair<string, string>> propertyNameDataTypes, string primaryKey)
        : CreateTable{StatementType::CREATE_NODE_CLAUSE, move(tableName),
              move(propertyNameDataTypes)},
          primaryKey{move(primaryKey)} {}

    inline string getPrimaryKey() const { return primaryKey; }

private:
    string primaryKey;
};

} // namespace parser
} // namespace graphflow
