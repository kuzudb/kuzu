#include <string>

#include "antlr4-runtime.h"

using namespace antlr4;
using namespace std;

namespace graphflow {
namespace parser {

class ParserErrorStrategy : public DefaultErrorStrategy {

protected:
    virtual void reportNoViableAlternative(Parser* recognizer, const NoViableAltException& e);
};

} // namespace parser
} // namespace graphflow
