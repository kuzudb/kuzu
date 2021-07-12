#include "src/binder/include/bound_statements/bound_load_csv_statement.h"
#include "src/binder/include/expression/function_expression.h"
#include "src/binder/include/expression/literal_expression.h"
#include "src/binder/include/expression/node_expression.h"
#include "src/binder/include/expression/property_expression.h"
#include "src/binder/include/expression/rel_expression.h"

using namespace graphflow::binder;

class BinderTestUtils {

public:
    static bool equals(const BoundLoadCSVStatement& left, const BoundLoadCSVStatement& right);

    static bool equals(const Expression& left, const Expression& right);

private:
    static bool equals(const PropertyExpression& left, const PropertyExpression& right);

    static bool equals(const FunctionExpression& left, const FunctionExpression& right);

    static bool equals(const RelExpression& left, const RelExpression& right);

    static bool equals(const NodeExpression& left, const NodeExpression& right);

    static bool equals(const LiteralExpression& left, const LiteralExpression& right);
};
