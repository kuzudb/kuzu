#include "src/binder/include/bound_statements/bound_load_csv_statement.h"

using namespace graphflow::binder;

class BinderTestUtils {

public:
    static bool equals(const BoundLoadCSVStatement& left, const BoundLoadCSVStatement& right);
};
