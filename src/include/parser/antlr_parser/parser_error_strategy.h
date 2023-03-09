#pragma once

#include <string>

#include "antlr4-runtime.h"

namespace kuzu {
namespace parser {

class ParserErrorStrategy : public antlr4::DefaultErrorStrategy {

protected:
    void reportNoViableAlternative(
        antlr4::Parser* recognizer, const antlr4::NoViableAltException& e) override;
};

} // namespace parser
} // namespace kuzu
