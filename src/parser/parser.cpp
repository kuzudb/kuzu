#include "parser/parser.h"

#include "cypher_lexer.h"
#include "parser/antlr_parser/kuzu_cypher_parser.h"
#include "parser/antlr_parser/parser_error_listener.h"
#include "parser/antlr_parser/parser_error_strategy.h"
#include "parser/transformer.h"

using namespace antlr4;

namespace kuzu {
namespace parser {

std::unique_ptr<Statement> Parser::parseQuery(const std::string& query) {
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
    kuzuCypherParser.setErrorHandler(std::make_shared<ParserErrorStrategy>());

    Transformer transformer(*kuzuCypherParser.oC_Cypher());
    return transformer.transform();
}

} // namespace parser
} // namespace kuzu
