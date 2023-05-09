#include "test_helper/test_case.h"
#include "test_helper/test_parser.h"

#include <iostream> // TODO: REMOVE ME
#include <numeric>

using namespace kuzu::common;
using namespace kuzu::planner;
using namespace kuzu::main;

namespace kuzu {
namespace testing {

// TODO: move to test_utils
std::string paramsToString(const std::vector<std::string>& params) {
    return std::accumulate(std::next(params.begin()), params.end(), std::string(),
            [](const std::string& a, const std::string& b) {
                return a + (a.empty() ? "" : " ") + b;
            }
    );
}

// TODO:
// parseTestFile (public):
//   openFile
//   parseHeader(&parser) (private, extract only GROUP, DATASET, NAME)
//   if parseHeader was ok:
//     parseBody(&parser) (private, parse the remaining script)
//
void TestCase::parseTestFile(const std::string& path) {
    TestCommand* currentCommand;
    TestParser parser;
    parser.openFile(path);
    bool commandStarted = false;

    while (parser.nextLine()) {
        parser.tokenize(); // Should we abstract this into nextLine()?
        if (parser.currentToken.type == TokenType::GROUP) {
            // checkMinimumParams(parser, 1);  
            group = parser.currentToken.params[1]; // parser.getParam(1); or parser.getRequiredParam(1); 
            continue; 
        } else if (parser.currentToken.type == TokenType::TEST) {
            // checkMinimumParams(parser, 1);  
            name = parser.currentToken.params[1];
            continue; 
        } else if (parser.currentToken.type == TokenType::DATASET) {
            // checkMinimumParams(parser, 1);  
            dataset = parser.currentToken.params[1];
            continue; 
        } else if (parser.currentToken.type == TokenType::NAME) {
            // initiate a new TestCommand
            auto command = std::make_unique<TestCommand>();
            currentCommand = command.get();
            commands.push_back(std::move(command));
            commandStarted = true;
            currentCommand->name = parser.currentToken.params[1];
            continue;
        } else if (parser.currentToken.type == TokenType::QUERY) {
            currentCommand->query = paramsToString(parser.currentToken.params);
            continue;


        } else if (parser.currentToken.type == TokenType::RESULT) {
            // checkMinimumParams(parser, 1);  
            std::string result = parser.currentToken.params[1];
            if (result == "ok") {
                currentCommand->expectedOk = true;
            } else if (result == "error") {
                currentCommand->expectedError = true;
                // read the error here
            } else {
                currentCommand->expectedNumTuples = stoi(result);
                for (auto i = 0u; i < currentCommand->expectedNumTuples; i++) {
                    parser.nextLine();
                    currentCommand->expectedTuples.push_back(parser.line);
                }
                if (!currentCommand->checkOutputOrder) { // order is not important for result
                    sort(currentCommand->expectedTuples.begin(), currentCommand->expectedTuples.end());
                }
            }
            


        } else if (parser.currentToken.type == TokenType::PARALLELISM) {
            // checkMinimumParams(parser, 1);  
            if (commandStarted) {
                currentCommand->numThreads = stoi(parser.currentToken.params[1]);
            }
            continue;
        } else if (parser.currentToken.type == TokenType::DEFINE_STATEMENT_BLOCK) {
            // Advance line by line
            // Check if it is statement, if not error
            // parse statement the same way as TokenType::STATEMENT
            // Add to statementVariables (name, vector of TestCommand) 
            continue;
        } else if (parser.currentToken.type == TokenType::STATEMENT_BLOCK) {
            // - STATEMENT_BLOCK create_rel_set
            // name create_rel_set in statementVariables
            // push all statements to result
        } else if (parser.currentToken.type == TokenType::LOOP) {
            // extract params:
            // begin = params[1]
            // end = params[2]
        } else if (parser.currentToken.type == TokenType::FOREACH) {
            // same as loop
        }
    }
}

} // namespace testing
} // namespace kuzu
