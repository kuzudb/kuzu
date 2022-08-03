#pragma once

#include "src/parser/ddl/include/ddl.h"

namespace graphflow {
namespace parser {

using namespace std;

struct RelConnection {
    RelConnection(vector<string> srcNodeLabels, vector<string> dstNodeLabels)
        : srcNodeLabels{move(srcNodeLabels)}, dstNodeLabels{move(dstNodeLabels)} {}
    vector<string> srcNodeLabels;
    vector<string> dstNodeLabels;
};

class CreateRelClause : public DDL {
public:
    explicit CreateRelClause(string labelName, vector<pair<string, string>> propertyNameDataTypes,
        string relMultiplicity, RelConnection relConnection)
        : DDL{StatementType::CREATE_REL_CLAUSE, move(labelName), move(propertyNameDataTypes)},
          relMultiplicity{move(relMultiplicity)}, relConnection{move(relConnection)} {}

    inline string getRelMultiplicity() const { return relMultiplicity; }

    inline RelConnection getRelConnection() const { return relConnection; }

private:
    string relMultiplicity;
    RelConnection relConnection;
};

} // namespace parser
} // namespace graphflow
