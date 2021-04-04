#include "src/parser/include/parser.h"

namespace graphflow {
namespace parser {

unique_ptr<SingleQuery> Parser::parseQuery(string query) {
    ANTLRInputStream inputStream(query);
    CypherLexer cypherLexer(&inputStream);
    CommonTokenStream tokens(&cypherLexer);
    tokens.fill();

    CypherParser cypherParser(&tokens);

    Transformer transformer(*cypherParser.oC_Cypher());
    return transformer.transform();
}

} // namespace parser
} // namespace graphflow
