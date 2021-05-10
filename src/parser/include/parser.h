#include "src/parser/include/queries/single_query.h"

namespace graphflow {
namespace parser {

class Parser {

public:
    static unique_ptr<SingleQuery> parseQuery(const string& query);
};

} // namespace parser
} // namespace graphflow
