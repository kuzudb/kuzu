#pragma once

#include <string>
#include <vector>

#include "src/parser/include/statement.h"

namespace graphflow {
namespace parser {

using namespace std;

class CreateNodeClause : public Statement {
public:
    explicit CreateNodeClause(
        vector<pair<string, string>> propertyNameDataTypes, string primaryKey, string labelName)
        : Statement{StatementType::CREATENODECLAUSE}, labelName{move(labelName)},
          primaryKey{move(primaryKey)}, propertyNameDataTypes{move(propertyNameDataTypes)} {}

    inline string getLabelName() const { return labelName; }
    inline string getPrimaryKey() const { return primaryKey; }
    inline vector<pair<string, string>> getPropertyNameDataTypes() const {
        return propertyNameDataTypes;
    }
    inline bool hasPrimaryKey() const { return primaryKey != ""; }

private:
    string labelName;
    string primaryKey;
    vector<pair<string, string>> propertyNameDataTypes;
};

} // namespace parser
} // namespace graphflow
