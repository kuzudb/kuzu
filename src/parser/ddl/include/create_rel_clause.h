#pragma once

#include "src/parser/ddl/include/ddl.h"

namespace graphflow {
namespace parser {

using namespace std;

struct RelConnection {
    RelConnection(vector<string> srcTableNames, vector<string> dstTableNames)
        : srcTableNames{move(srcTableNames)}, dstTableNames{move(dstTableNames)} {}
    vector<string> srcTableNames;
    vector<string> dstTableNames;
};

class CreateRelClause : public DDL {
public:
    explicit CreateRelClause(string tableName, vector<pair<string, string>> propertyNameDataTypes,
        string relMultiplicity, RelConnection relConnection)
        : DDL{StatementType::CREATE_REL_CLAUSE, move(tableName), move(propertyNameDataTypes)},
          relMultiplicity{move(relMultiplicity)}, relConnection{move(relConnection)} {}

    inline string getRelMultiplicity() const { return relMultiplicity; }

    inline RelConnection getRelConnection() const { return relConnection; }

private:
    string relMultiplicity;
    RelConnection relConnection;
};

} // namespace parser
} // namespace graphflow
