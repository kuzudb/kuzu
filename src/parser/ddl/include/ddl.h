#pragma once

#include <string>
#include <vector>

#include "src/parser/include/statement.h"

namespace graphflow {
namespace parser {

using namespace std;

class DDL : public Statement {
public:
    explicit DDL(StatementType statementType, string labelName,
        vector<pair<string, string>> propertyNameDataTypes)
        : Statement{statementType}, labelName{move(labelName)}, propertyNameDataTypes{
                                                                    move(propertyNameDataTypes)} {}

    inline string getLabelName() const { return labelName; }
    inline vector<pair<string, string>> getPropertyNameDataTypes() const {
        return propertyNameDataTypes;
    }

protected:
    string labelName;
    vector<pair<string, string>> propertyNameDataTypes;
};

} // namespace parser
} // namespace graphflow
