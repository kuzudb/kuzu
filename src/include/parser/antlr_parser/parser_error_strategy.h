#pragma once

#include <string>

#include "antlr4-runtime.h"

using namespace antlr4;
using namespace std;

namespace kuzu {
namespace parser {

class ParserErrorStrategy : public DefaultErrorStrategy {

protected:
    void reportNoViableAlternative(Parser* recognizer, const NoViableAltException& e);
};

} // namespace parser
} // namespace kuzu
