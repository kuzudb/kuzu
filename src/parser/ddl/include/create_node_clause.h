#pragma once

#include "src/parser/ddl/include/create_table.h"

namespace graphflow {
namespace parser {

using namespace std;

class CreateNodeClause : public CreateTable {
public:
    explicit CreateNodeClause(
        string tableName, vector<pair<string, string>> propertyNameDataTypes, string pkColName)
        : CreateTable{StatementType::CREATE_NODE_CLAUSE, move(tableName),
              move(propertyNameDataTypes)},
          pKColName{move(pkColName)} {}

    inline string getPKColName() const { return pKColName; }

private:
    string pKColName;
};

} // namespace parser
} // namespace graphflow
