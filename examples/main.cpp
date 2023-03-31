#include "main/database.h"
#include "main/connection.h"
#include "main/prepared_statement.h"
#include "binder/bound_statement_result.h"
#include "planner/logical_plan/logical_plan.h"

#include <iostream>

using namespace kuzu::main;
using namespace kuzu::common;

void testPyG(Connection* conn, const std::string& srcTableName,
    const std::string& relName, const std::string& dstTableName, size_t queryBatchSize) {
    // Get the number of nodes in the src table for batching.
    auto numNodesQuery = "MATCH (a:{}) RETURN count(*)";
    auto numNodesQueryWithParams = StringUtils::string_format(numNodesQuery, srcTableName);
    std::cout << "Execute query: " << numNodesQueryWithParams << std::endl;
    auto numNodesResult = conn->query(numNodesQueryWithParams);
    std::cout << "Done execute query: " << numNodesQueryWithParams << std::endl;

    if (!numNodesResult->isSuccess()) {
        throw std::runtime_error(numNodesResult->getErrorMessage());
    }
    auto numNodes = numNodesResult->getNext()->getValue(0)->getValue<int64_t>();
    int64_t batches = numNodes / queryBatchSize;
    if (numNodes % queryBatchSize != 0) {
        batches += 1;
    }

    // Get total number of edges for allocating the buffer.
    auto countQuery = "MATCH (a:{})-[:{}]->(b:{}) RETURN count(*)";
    auto countQueryWithParams =
        StringUtils::string_format(countQuery, srcTableName, relName, dstTableName);
    std::cout << "Execute query: " << countQueryWithParams << std::endl;
    auto countResult = conn->query(countQueryWithParams);
    std::cout << "Done execute query: " << countQueryWithParams << std::endl;

    if (!countResult->isSuccess()) {
        throw std::runtime_error(countResult->getErrorMessage());
    }
    uint64_t count = countResult->getNext()->getValue(0)->getValue<int64_t>();
    std::cout << "Allocate buffer." << std::endl;
    uint64_t bufferSize = count * 2 * sizeof(int64_t);
    auto* buffer = (int64_t*)malloc(bufferSize);
    memset(buffer, 0, bufferSize);
    std::cout << "Done allocating buffer." << std::endl;
    // Run queries in batch to fetch edges.
    auto queryString = "MATCH (a:{})-[:{}]->(b:{}) WHERE offset(id(a)) >= $s AND offset(id(a)) < "
                       "$e RETURN offset(id(a)), offset(id(b))";
    auto query = StringUtils::string_format(queryString, srcTableName, relName, dstTableName);
    std::cout << "Prepare query: " << query << std::endl;
    auto preparedStatement = conn->prepare(query);
    uint64_t i = 0;
    for (int64_t batch = 0; batch < batches; ++batch) {
        int64_t start = batch * queryBatchSize;
        int64_t end = (batch + 1) * queryBatchSize;
        end = end > numNodes ? numNodes : end;
        std::unordered_map<std::string, std::shared_ptr<Value>> parameters;
        parameters["s"] = std::make_shared<Value>(start);
        parameters["e"] = std::make_shared<Value>(end);
        std::cout << "Execute query with params " << start << ", " << end << std::endl;
        auto result = conn->executeWithParams(preparedStatement.get(), parameters);
        std::cout << "Done execute query with params " << start << ", " << end << std::endl;
        if (!result->isSuccess()) {
            throw std::runtime_error(result->getErrorMessage());
        }
        while (result->hasNext()) {
            auto row = result->getNext();
            buffer[i] = row->getValue(0)->getValue<int64_t>();
            buffer[i + count] = row->getValue(1)->getValue<int64_t>();
            i += 1;
        }
    }
}

int main() {
    std::string dbPath = "/home/lc/Developer/mag100m-kuzu";
    // std::string dbPath = "/home/kuzu/Developer/mag100m-kuzu";
    kuzu::main::SystemConfig systemConfig;
    systemConfig.bufferPoolSize = (std::uint64_t)10 * 1024 * 1024 * 1024;
    auto database = std::make_unique<Database>(dbPath, systemConfig);
    auto conn = std::make_unique<Connection>(database.get());
    testPyG(conn.get(), "Paper", "Cites", "Paper", 1000000);
    std::cout << "Done processing! Now we wait" << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(1000));
}
