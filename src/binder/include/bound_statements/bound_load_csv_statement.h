#pragma once

#include "src/binder/include/bound_statements/bound_reading_statement.h"
#include "src/binder/include/expression/variable_expression.h"
#include "src/common/include/types.h"

using namespace graphflow::common;

namespace graphflow {
namespace binder {

/**
 * Assume csv file contains structured information (data type is specified in the header).
 */
class BoundLoadCSVStatement : public BoundReadingStatement {

public:
    BoundLoadCSVStatement(string filePath, char tokenSeparator,
        vector<shared_ptr<VariableExpression>> csvColumnVariables)
        : BoundReadingStatement{LOAD_CSV_STATEMENT}, filePath{move(filePath)},
          tokenSeparator(tokenSeparator), csvColumnVariables{move(csvColumnVariables)} {}

    vector<shared_ptr<Expression>> getIncludedVariables() const override {
        return vector<shared_ptr<Expression>>();
    }

public:
    string filePath;
    char tokenSeparator;
    vector<shared_ptr<VariableExpression>> csvColumnVariables;
};

} // namespace binder
} // namespace graphflow
