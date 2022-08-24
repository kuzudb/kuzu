#pragma once

#include <string>
#include <vector>

#include "src/parser/include/statement.h"

namespace graphflow {
namespace parser {

using namespace std;

class DDL : public Statement {
public:
    explicit DDL(StatementType statementType, string tableName,
        vector<pair<string, string>> propertyNameDataTypes)
        : Statement{statementType}, tableName{move(tableName)}, propertyNameDataTypes{
                                                                    move(propertyNameDataTypes)} {}

    inline string getTableName() const { return tableName; }
    inline vector<pair<string, string>> getPropertyNameDataTypes() const {
        return propertyNameDataTypes;
    }

protected:
    string tableName;
    vector<pair<string, string>> propertyNameDataTypes;
};

} // namespace parser
} // namespace graphflow
