#include "src/parser/include/parser.h"

namespace graphflow {
namespace parser {

unique_ptr<SingleQuery> Parser::parseQuery(string query) {
    ANTLRInputStream inputStream(query);
    CypherLexer cypherLexer(&inputStream);
    CommonTokenStream tokens(&cypherLexer);
    tokens.fill();

    CypherParser cypherParser(&tokens);
    CypherParser::OC_CypherContext* tree = cypherParser.oC_Cypher();

    Transformer transformer;
    return transformer.transformParseTree(tree);
}

} // namespace parser
} // namespace graphflow
