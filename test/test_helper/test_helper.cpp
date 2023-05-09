#include "test_helper/test_helper.h"
#include "test_helper/test_parser.h"

#include <fstream>

#include "json.hpp"
#include "spdlog/spdlog.h"

using namespace kuzu::common;
using namespace kuzu::planner;
using namespace kuzu::main;

namespace kuzu {
namespace testing {

// TODO: REMOVE ME
TestConfig TestHelper::parseGroupFile(const std::string& path) {
    TestConfig result;
    if (!FileUtils::fileOrPathExists(path)) {
        return result;
    }
    std::ifstream ifs(path);
    std::string line;
    while (getline(ifs, line)) {
        setConfigValue(line, result.testGroup, "-GROUP");
        setConfigValue(line, result.testName, "-TEST");
        setConfigValue(line, result.dataset, "-DATASET");
        setConfigValue(line, result.checkOrder, "-CHECK_ORDER");
    }
    return result;
}

// TODO: REMOVE ME
void TestHelper::setConfigValue(
    const std::string& line, std::string& configItem, const std::string& configKey) {
    std::string value = extractConfigValue(line, configKey);
    if (!value.empty())
        configItem = value;
}

// TODO: REMOVE ME
void TestHelper::setConfigValue(
    const std::string& line, bool& configItem, const std::string& configKey) {
    std::string value = extractConfigValue(line, configKey);
    if (!value.empty())
        configItem = (value == "TRUE");
}

// TODO: REMOVE ME
std::string TestHelper::extractConfigValue(const std::string& line, const std::string& configKey) {
    std::string value;
    if (line.starts_with(configKey)) {
        value = line.substr(configKey.length() + 1, line.length());
        value.erase(remove_if(value.begin(), value.end(), isspace), value.end());
    }
    return value;
}


/* runTest (graph_test) : call this function to generate the vector of TestQueryConfig
 * testQueries (TestHelper) : go over each TestQueryConfig and check
 * 
 * [] Multi-statement:
 * DEFINE STATEMENT_BLOCK: vector of TestQueryConfig. Delay the execution
 * STATEMENT_BLOCK : find the statement block and execute. Throw
 *                   exception if not found
 *
 * [] indicator ok/error
 * change uint64_t numTuples to auto/template?
 *
 * [] csv check
 * add a expectedTupleOutput (text/csv file)?
 *
 * [] shell command
 * -COMMAND statement
 *  interpret and call the associated function (example
 *  connection->setTimeOut)
 *
 */


// Parse the entire test file -> add to a sequential vector of TestCommands -> Run testCommands sequentially
std::vector<std::unique_ptr<TestQueryConfig>> TestHelper::parseTestFile(
    const std::string& path, bool checkOutputOrder) {
    std::vector<std::unique_ptr<TestQueryConfig>> result;
    TestQueryConfig* currentConfig;
    TestParser parser;
    parser.openFile(path);

    while (parser.nextLine()) {
        parser.tokenize();

        if (parser.currentToken.type == TokenType::NAME) {
            continue;
        } else if (parser.currentToken.type == TokenType::STATEMENT) {
            // parse statement and store it 
            // add to testCommands
            continue;
        } else if (parser.currentToken.type == TokenType::PARALLELISM) {
            // check the number of parameters, error if wrong
            currentConfig->numThreads = stoi(parser.currentToken.params[1]);
            continue;
        } else if (parser.currentToken.type == TokenType::DEFINE_STATEMENT_BLOCK) {
/*
- DEFINE STATEMENT_BLOCK create_rel_set [
    - STATEMENT MATCH (a:person), (b:person) WHERE a.ID=10 AND b.ID=20 CREATE (a)-[e:knows]->(b);
    ---- ok
    - STATEMENT MATCH (a:person), (b:person) WHERE a.ID=1 AND b.ID=2 CREATE (a)-[e:knows]->(b);
    ---- ok
    - STATEMENT MATCH (a:person), (b:person) WHERE a.ID=1 AND b.ID=20 CREATE (a)-[e:knows]->(b);
    ---- error
    "Exception: Duplicate primary key"
]
*/

            // Extract the content inside [] 
            // Add to statementVariableMap (name, vector of TestQueryConfig) 
            continue;
        } else if (parser.currentToken.type == TokenType::STATEMENT_BLOCK) {
            // - STATEMENT_BLOCK create_rel_set
            // name = params[1] = create_rel_set
            // push all statements to result
        } else if (parser.currentToken.type == TokenType::LOOP) {
            // extract params:
            // begin = params[1]
            // end = params[2]
        } else if (parser.currentToken.type == TokenType::FOREACH) {
        }



/*
        if (line.starts_with("-NAME")) {
            auto config = std::make_unique<TestQueryConfig>();
            currentConfig = config.get();
            result.push_back(std::move(config));
            currentConfig->name = line.substr(6, line.length());
        } else if (line.starts_with("-QUERY")) {
            currentConfig->query = line.substr(7, line.length());
        } else if (line.starts_with("-PARALLELISM")) {
            currentConfig->numThreads = stoi(line.substr(13, line.length()));
        } else if (line.starts_with("-ENCODED_JOIN")) {
            currentConfig->encodedJoin = line.substr(14, line.length());
        } else if (line.starts_with("-ENUMERATE")) {
            currentConfig->enumerate = true;
        } else if (line.starts_with("----")) {
            uint64_t numTuples = stoi(line.substr(5, line.length()));
            currentConfig->expectedNumTuples = numTuples;
            for (auto i = 0u; i < numTuples; i++) {
                parser.nextLine();
                line = parser.line;
                currentConfig->expectedTuples.push_back(line);
            }
            if (!checkOutputOrder) { // order is not important for result
                sort(currentConfig->expectedTuples.begin(), currentConfig->expectedTuples.end());
            }
        }
*/


    }
    return result;
}

/*
 * 
 *
 *
 *
 */
bool TestHelper::testQueries(
    std::vector<std::unique_ptr<TestQueryConfig>>& configs, Connection& conn) {
    spdlog::set_level(spdlog::level::info);
    auto numPassedQueries = 0u;
    std::vector<uint64_t> failedQueries;
    for (auto i = 0u; i < configs.size(); i++) {
        if (testQuery(configs[i].get(), conn)) {
            numPassedQueries++;
        } else {
            failedQueries.push_back(i);
        }
    }
    spdlog::info("SUMMARY:");
    if (failedQueries.empty()) {
        spdlog::info("ALL QUERIES PASSED.");
    } else {
        for (auto& idx : failedQueries) {
            spdlog::info("QUERY {} NOT PASSED.", configs[idx]->name);
        }
    }
    return numPassedQueries == configs.size();
}

std::vector<std::string> TestHelper::convertResultToString(
    QueryResult& queryResult, bool checkOutputOrder) {
    std::vector<std::string> actualOutput;
    while (queryResult.hasNext()) {
        auto tuple = queryResult.getNext();
        actualOutput.push_back(tuple->toString(std::vector<uint32_t>(tuple->len(), 0)));
    }
    if (!checkOutputOrder) {
        sort(actualOutput.begin(), actualOutput.end());
    }
    return actualOutput;
}

void TestHelper::initializeConnection(TestQueryConfig* config, Connection& conn) {
    spdlog::info("TEST: {}", config->name);
    spdlog::info("QUERY: {}", config->query);
    conn.setMaxNumThreadForExec(config->numThreads);
}

bool TestHelper::testQuery(TestQueryConfig* config, Connection& conn) {
    initializeConnection(config, conn);
    std::unique_ptr<PreparedStatement> preparedStatement;
    if (config->encodedJoin.empty()) {
        preparedStatement = conn.prepareNoLock(config->query, config->enumerate);
    } else {
        preparedStatement =
            conn.prepareNoLock(config->query, true /* enumerate*/, config->encodedJoin);
    }
    if (!preparedStatement->isSuccess()) {
        spdlog::error(preparedStatement->getErrorMessage());
        return false;
    }
    auto numPlans = preparedStatement->logicalPlans.size();
    if (numPlans == 0) {
        spdlog::error("Query {} has no plans" + config->name);
        return false;
    }
    auto numPassedPlans = 0u;
    for (auto i = 0u; i < numPlans; ++i) {
        auto planStr = preparedStatement->logicalPlans[i]->toString();
        auto result = conn.executeAndAutoCommitIfNecessaryNoLock(preparedStatement.get(), i);
        assert(result->isSuccess());
        std::vector<std::string> resultTuples =
            convertResultToString(*result, config->checkOutputOrder);
        if (resultTuples.size() == result->getNumTuples() &&
            resultTuples == config->expectedTuples) {
            spdlog::info(
                "PLAN{} PASSED in {}ms.", i, result->getQuerySummary()->getExecutionTime());
            numPassedPlans++;
        } else {
            spdlog::error("PLAN{} NOT PASSED.", i);
            spdlog::info("PLAN: \n{}", planStr);
            spdlog::info("RESULT: \n");
            for (auto& tuple : resultTuples) {
                spdlog::info(tuple);
            }
        }
    }
    spdlog::info("{}/{} plans passed.", numPassedPlans, numPlans);
    return numPassedPlans == numPlans;
}

std::unique_ptr<planner::LogicalPlan> TestHelper::getLogicalPlan(
    const std::string& query, kuzu::main::Connection& conn) {
    return std::move(conn.prepare(query)->logicalPlans[0]);
}

} // namespace testing
} // namespace kuzu
