#include "src/parser/include/queries/regular_query.h"

namespace graphflow {
namespace parser {

class Parser {

public:
    static unique_ptr<RegularQuery> parseQuery(const string& query);
};

} // namespace parser
} // namespace graphflow
