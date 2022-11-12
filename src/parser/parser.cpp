#include "src/parser/include/parser.h"

#include "src/antlr4/CypherLexer.h"
#include "src/parser/antlr_parser/include/kuzu_cypher_parser.h"
#include "src/parser/antlr_parser/include/parser_error_listener.h"
#include "src/parser/antlr_parser/include/parser_error_strategy.h"
#include "src/parser/include/transformer.h"

using namespace antlr4;

namespace kuzu {
namespace parser {

unique_ptr<Statement> Parser::parseQuery(const string& query) {
    auto inputStream = ANTLRInputStream(query);
    auto parserErrorListener = ParserErrorListener();

    auto cypherLexer = CypherLexer(&inputStream);
    cypherLexer.removeErrorListeners();
    cypherLexer.addErrorListener(&parserErrorListener);
    auto tokens = CommonTokenStream(&cypherLexer);
    tokens.fill();

    auto kuzuCypherParser = KuzuCypherParser(&tokens);
    kuzuCypherParser.removeErrorListeners();
    kuzuCypherParser.addErrorListener(&parserErrorListener);
    kuzuCypherParser.setErrorHandler(make_shared<ParserErrorStrategy>());

    Transformer transformer(*kuzuCypherParser.oC_Cypher());
    return transformer.transform();
}

} // namespace parser
} // namespace kuzu
