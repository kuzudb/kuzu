#include <algorithm>
#include <chrono>
#include <iostream>
#include <random>
#include <thread>

#include "main/connection.h"
#include "storage/storage_manager.h"
#include "storage/store/node_table.h"
#include "storage/store/table.h"

using namespace kuzu::common;
using namespace kuzu::main;
using namespace kuzu::storage;

void lookup(const std::vector<uint64_t>& ids, int64_t start, int64_t end, Database* db) {
    auto connection = std::make_unique<Connection>(db);
    connection->query("BEGIN TRANSACTION READ ONLY");
    auto transaction = connection->getClientContext()->getTransaction();
    auto& table =
        connection->getClientContext()->getStorageManager()->getTable(1)->cast<NodeTable>();
    std::cout << table.getTableName() << std::endl;
    auto dataChunk = std::make_unique<DataChunk>(2);
    auto nodeIDVector = std::make_shared<ValueVector>(LogicalType::INTERNAL_ID());
    dataChunk->insert(0, nodeIDVector);
    dataChunk->state->getSelVectorUnsafe().setSelSize(1);
    auto resultVector =
        std::make_shared<ValueVector>(LogicalType::ARRAY(LogicalType::FLOAT(), 960));
    dataChunk->insert(1, resultVector);
    std::vector<ValueVector*> outputVectors;
    outputVectors.push_back(resultVector.get());
    auto scanState =
        std::make_unique<NodeTableScanState>(nodeIDVector.get(), outputVectors, dataChunk->state);
    scanState->setToTable(transaction, &table, {1}, {});
    scanState->source = TableScanSource::COMMITTED;
    for (auto i = start; i < end; i++) {
        auto id = ids[i];
        scanState->nodeIDVector->setValue<nodeID_t>(0, nodeID_t{id, table.getTableID()});
        scanState->nodeGroupIdx = StorageUtils::getNodeGroupIdx(id);
        table.initScanState(transaction, *scanState, table.getTableID());
        [[maybe_unused]] auto res = table.lookup(transaction, *scanState);
        // connection->query(
        // "MATCH (t:gist_tbl) WHERE t.id=" + std::to_string(id) + " RETURN t.vectors;");
    }
    connection->query("COMMIT;");
}

int main() {
    SystemConfig systemConfig;
    systemConfig.maxNumThreads = 8;
    auto database =
        std::make_unique<Database>("/Users/guodongjin/Downloads/kuvector" /* */, systemConfig);
    auto connection = std::make_unique<Connection>(database.get());

    // connection->query("CREATE NODE TABLE tbl (id int64 PRIMARY KEY, vectors FLOAT[96]);");
    // connection->query(
    // "COPY tbl FROM '/Users/guodongjin/Downloads/ann-benchmark/deep-10M/*.parquet';");
    // Generate random ids.
    unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
    std::mt19937 generator(seed); // Mersenne Twister engine
    std::uniform_int_distribution distribution(0, 1000000 - 1);

    std::vector<uint64_t> ids;
    ids.reserve(1000000);
    for (auto i = 0; i < 1000000; i++) {
        ids.push_back(distribution(generator));
    }

    // Execute a simple query.
    std::vector<std::thread> threads;
    Timer timer;
    timer.start();
    for (auto i = 0; i < systemConfig.maxNumThreads; i++) {
        auto stride = 1000000 / systemConfig.maxNumThreads;
        auto start = i * stride;
        auto end = (i + 1) * stride;
        threads.emplace_back(lookup, std::ref(ids), start, end, database.get());
    }
    for (auto& thread : threads) {
        thread.join();
    }
    auto elapsed = timer.getElapsedTimeInMS();
    std::cout << "Elapsed time: " << elapsed << " ms" << std::endl;
}
