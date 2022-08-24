#include <algorithm>
#include <fstream>
#include <iostream>
#include <string>

#include "tools/join_order_pick/include/jo_connection.h"

#include "src/main/include/connection.h"
#include "src/main/include/database.h"
#include "src/planner/logical_plan/include/logical_plan_util.h"

using namespace graphflow::main;

// trim from start (in place)
static inline void ltrim(std::string& s) {
    s.erase(s.begin(),
        std::find_if(s.begin(), s.end(), [](unsigned char ch) { return !std::isspace(ch); }));
}

// trim from end (in place)
static inline void rtrim(std::string& s) {
    s.erase(std::find_if(s.rbegin(), s.rend(), [](unsigned char ch) { return !std::isspace(ch); })
                .base(),
        s.end());
}

// trim from both ends (in place)
static inline void trim(std::string& s) {
    ltrim(s);
    rtrim(s);
}

static string ReadFile(const string& file_path) {
    ifstream ifs(file_path);
    std::string str(std::istreambuf_iterator<char>{ifs}, {});
    return str;
}

unique_ptr<QueryResult> run(JOConnection* conn, const string& profiledQuery, string& encodedJoin) {
    StringUtils::toLower(encodedJoin);
    if (encodedJoin == "optimizer") {
        return conn->query(profiledQuery);
    } else if (encodedJoin == "3hop") {
        auto plan = conn->getThreeHopPlan(profiledQuery);
        return conn->executePlan(move(plan));
    } else if (encodedJoin == "is02") {
        auto plan = conn->getIS02Plan(profiledQuery);
        return conn->executePlan(move(plan));
    } else if (encodedJoin == "is06") {
        auto plan = conn->getIS06Plan(profiledQuery);
        return conn->executePlan(move(plan));
    } else if (encodedJoin == "is07") {
        auto plan = conn->getIS07Plan(profiledQuery);
        return conn->executePlan(move(plan));
    } else if (encodedJoin == "triangle") {
        auto plan = conn->getTrianglePlan(profiledQuery);
        return conn->executePlan(move(plan));
    } else if (encodedJoin == "cycle") {
        auto plan = conn->getCyclePlan(profiledQuery);
        return conn->executePlan(move(plan));
    } else if (encodedJoin == "clique") {
        auto plan = conn->getCliquePlan(profiledQuery);
        return conn->executePlan(move(plan));
    } else {
        return conn->query(profiledQuery, encodedJoin);
    }
}

void runQuery(const string& serializedPath, const string& queryPath, string& encodedJoin,
    uint64_t numThreads) {
    assert(numThreads <= 8);
    auto query = ReadFile(queryPath);
    DatabaseConfig databaseConfig(serializedPath);
    SystemConfig systemConfig;
    systemConfig.maxNumThreads = 8;
    systemConfig.defaultPageBufferPoolSize = 1ull << 34;
    systemConfig.largePageBufferPoolSize = 1ull << 34;
    auto database = make_unique<Database>(databaseConfig, systemConfig);
    auto conn = make_unique<JOConnection>(database.get());
    conn->setMaxNumThreadForExec(numThreads);
    auto profileQuery = "PROFILE " + query;
    unique_ptr<QueryResult> result = run(conn.get(), profileQuery, encodedJoin);
    if (!result->isSuccess()) {
        cout << "Error: " << result->getErrorMessage() << endl;
        return;
    }
    cout << "Num tuples: " << result->getNumTuples() << endl;
    vector<uint32_t> widths(result->getNumColumns(), 10);
    while (result->hasNext()) {
        auto tuple = result->getNext();
        cout << tuple->toString(widths, "|") << endl;
    }
    auto querySummary = result->getQuerySummary();
    cout << "Time: " << querySummary->getExecutionTime() << " ms." << endl;
    if (querySummary->getIsProfile()) {
        querySummary->printPlanToStdOut();
    } else {
        cout << "No profiling available." << endl;
    }
    vector<double> runTimes(5);
    for (auto i = 0u; i < 5; i++) {
        result.reset();
        result = run(conn.get(), profileQuery, encodedJoin);
        runTimes[i] = result->getQuerySummary()->getExecutionTime();
    }
    cout << "Cost: ";
    double sumTime = 0;
    for (auto i = 0u; i < 5; i++) {
        cout << runTimes[i] << " ";
        sumTime += runTimes[i];
    }
    cout << "Avg: " << sumTime / (double)5 << " ms." << endl;
}

void planEnumerator(const string& query) {
    string inputPath = "/Users/g35jin/Downloads/ldbc-0.1/";
    string serializedPath = inputPath + "/serialized";
    DatabaseConfig databaseConfig(serializedPath);
    SystemConfig systemConfig;
    systemConfig.maxNumThreads = 1;
    auto database = make_unique<Database>(databaseConfig, systemConfig);
    auto conn = make_unique<Connection>(database.get());
    conn->setMaxNumThreadForExec(1);
    auto plans = conn->enumeratePlans(query);
    auto planID = 0;
    for (auto& plan : plans) {
        cout << "PLAN " << planID << endl;
        auto planStr = plan->getLastOperator()->toString();
        cout << planStr << endl;
        auto encodedStr = graphflow::planner::LogicalPlanUtil::encodeJoin(*plan);
        cout << encodedStr << endl;
        planID++;
    }
}

// Q1. Plan: 3 HJ(b){E(b)S(a)}{S(b)}. Count: 1938516
// Q2. Plan: 47 HJ(c){HJ(a){E(c)E(a)S(b)}{S(a)}}{S(c)}. Count: 149008866
int main(int argv, char* argc[]) {
    if (argv != 5) {
        cout << "HELP: EXEC <serializedPath> <queryPath> <encodedJoin> <numThreads>" << endl;
        return 1;
    }
    auto serializedPath = argc[1];
    auto queryPath = argc[2];
    string encodedJoin = argc[3];
    std::replace(encodedJoin.begin(), encodedJoin.end(), '\"', ' ');
    trim(encodedJoin);
    auto numThreads = stoi(argc[4]);
    runQuery(serializedPath, queryPath, encodedJoin, numThreads);
}
