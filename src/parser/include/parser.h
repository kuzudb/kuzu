#include "src/parser/include/queries/single_query.h"

namespace graphflow {
namespace parser {

class Parser {

public:
    unique_ptr<SingleQuery> parseQuery(string query);
};

} // namespace parser
} // namespace graphflow
