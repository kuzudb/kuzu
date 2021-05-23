#include "src/parser/include/queries/single_query.h"
#include "src/parser/include/statements/load_csv_statement.h"
#include "src/parser/include/statements/match_statement.h"

using namespace graphflow::parser;

const string EMPTY = string();

class ParserTestUtils {

public:
    static bool equals(const LoadCSVStatement& left, const LoadCSVStatement& right);
    static bool equals(const MatchStatement& left, const MatchStatement& right);
    static bool equals(const WithStatement& left, const WithStatement& right);
    static bool equals(const ReturnStatement& left, const ReturnStatement& right);
    static bool equals(const ParsedExpression& left, const ParsedExpression& right);

private:
    static bool equals(const PatternElement& left, const PatternElement& right);
    static bool equals(const PatternElementChain& left, const PatternElementChain& right);
    static bool equals(const RelPattern& left, const RelPattern& right);
    static bool equals(const NodePattern& left, const NodePattern& right);
};
