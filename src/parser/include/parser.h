#include "antlr4-runtime.h"

#include "src/antlr4/CypherLexer.h"
#include "src/antlr4/CypherParser.h"
#include "src/parser/include/transformer.h"

using namespace antlr4;

namespace graphflow {
namespace parser {

class Parser {

public:
    unique_ptr<SingleQuery> parseQuery(string query);
};

} // namespace parser
} // namespace graphflow
