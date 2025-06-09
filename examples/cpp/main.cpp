#include <assert.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

#include "kuzu.hpp"
using namespace kuzu::main;

void deleteDir() {
    namespace fs = std::filesystem;
    fs::remove_all("/tmp/bench");
}

void createFTS(Connection* connection) {
    auto result =
        connection->query("CREATE NODE TABLE doc(docid int64, content string, primary key(docid));")
            ->isSuccess();
    assert(result);
    result =
        connection
            ->query("copy doc from (load from '/Users/z473chen/Desktop/code/kuzu/doc_first.csv' "
                    "(file_format='csv', delim = '\\\\t') return * limit 100000)")
            ->isSuccess();
    assert(result);
    result =
        connection->query("CALL CREATE_FTS_INDEX('doc', 'contentIdx',['content']);")->isSuccess();
    assert(result);
}

void insertFTS(Connection* connection) {
    auto start = std::chrono::high_resolution_clock::now();
    auto result = connection->query(
        "load from \"/Users/z473chen/Desktop/code/kuzu/insert.csv\"(delim = '\\\\t') "
        "with column0 as "
        "t0, column1 as t1 limit 10000 create (d:doc {docid: t0, content: t1})");
    printf("%s", result->toString().c_str());
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> duration = end - start;
    std::cout << "Time taken: " << duration.count() << " ms" << std::endl;
}

int main() {
    //deleteDir();
    auto database = std::make_unique<Database>("/tmp/bench" /* fill db path */);
    auto connection = std::make_unique<Connection>(database.get());
    connection->query(
        "load extension "
        "\"/Users/z473chen/Desktop/code/kuzu/extension/fts/build/libfts.kuzu_extension\"");
    //createFTS(connection.get());
    insertFTS(connection.get());
}
