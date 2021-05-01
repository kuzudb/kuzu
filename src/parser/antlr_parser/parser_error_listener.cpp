#include "src/parser/include/antlr_parser/parser_error_listener.h"

namespace graphflow {
namespace parser {

static vector<string> splitString(const string& str, const string& delimiter);

void ParserErrorListener::syntaxError(Recognizer* recognizer, Token* offendingSymbol, size_t line,
    size_t charPositionInLine, const std::string& msg, std::exception_ptr e) {
    auto finalError = msg + " (line: " + to_string(line) +
                      ", offset: " + to_string(charPositionInLine) + ")\n" +
                      formatUnderLineError(*recognizer, *offendingSymbol, line, charPositionInLine);
    throw invalid_argument(finalError);
}

string ParserErrorListener::formatUnderLineError(
    Recognizer& recognizer, const Token& offendingToken, size_t line, size_t charPositionInLine) {
    auto tokens = (CommonTokenStream*)recognizer.getInputStream();
    auto input = tokens->getTokenSource()->getInputStream()->toString();
    auto errorLine = splitString(input, "\n")[line - 1];
    auto underLine = string(" ");
    for (auto i = 0u; i < charPositionInLine; ++i) {
        underLine += " ";
    }
    for (auto i = offendingToken.getStartIndex(); i <= offendingToken.getStopIndex(); ++i) {
        underLine += "^";
    }
    return "\"" + errorLine + "\"\n" + underLine;
}

vector<string> splitString(const string& str, const string& delimiter) {
    auto strings = vector<string>();
    auto prevPos = 0u;
    auto currentPos = str.find(delimiter, prevPos);
    while (currentPos != string::npos) {
        strings.push_back(str.substr(prevPos, currentPos - prevPos));
        prevPos = currentPos + 1;
        currentPos = str.find(delimiter, prevPos);
    }
    strings.push_back(str.substr(prevPos));
    return strings;
}

} // namespace parser
} // namespace graphflow
