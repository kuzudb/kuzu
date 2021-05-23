#pragma once

#include "src/binder/include/bound_statements/bound_reading_statement.h"
#include "src/binder/include/expression/expression.h"
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
        vector<pair<string, DataType>> headerInfo, shared_ptr<Expression> csvLineVariable)
        : BoundReadingStatement{LOAD_CSV_STATEMENT}, filePath{move(filePath)},
          tokenSeparator(tokenSeparator), headerInfo{move(headerInfo)}, csvLineVariable{move(
                                                                            csvLineVariable)} {}

public:
    string filePath;
    char tokenSeparator;
    vector<pair<string, DataType>> headerInfo;
    shared_ptr<Expression> csvLineVariable;
};

} // namespace binder
} // namespace graphflow
