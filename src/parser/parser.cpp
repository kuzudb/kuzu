#include "parser/parser.h"

// ANTLR4 generates code with unused parameters.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include "cypher_lexer.h"
#pragma GCC diagnostic pop

#include "parser/antlr_parser/kuzu_cypher_parser.h"
#include "parser/antlr_parser/parser_error_listener.h"
#include "parser/antlr_parser/parser_error_strategy.h"
#include "parser/transformer.h"

using namespace antlr4;

namespace kuzu {
namespace parser {

std::vector<std::shared_ptr<Statement>> Parser::parseQuery(std::string_view query) {
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

    Transformer transformer(*kuzuCypherParser.ku_Statements());
    return transformer.transform();
}

} // namespace parser
} // namespace kuzu
