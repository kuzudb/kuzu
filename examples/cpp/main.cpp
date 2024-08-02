#include <iostream>
#include <thread>
#include <vector>

#include "kuzu.hpp"

using namespace kuzu::main;

static void execute(Connection* conn) {
    printf("%s", conn->query("LOAD FROM 's3://kuzu/ldbc_part0.csv' return *;")->toString().c_str());
}

int main() {
    auto database = std::make_unique<Database>("abc" /* fill db path */);
    auto connection1 = std::make_unique<Connection>(database.get());
    auto connection2 = std::make_unique<Connection>(database.get());
    connection1->query(
        "load extension "
        "'/Users/z473chen/Desktop/code/kuzu/extension/httpfs/build/libhttpfs.kuzu_extension'");
    connection2->query(
        "load extension "
        "'/Users/z473chen/Desktop/code/kuzu/extension/httpfs/build/libhttpfs.kuzu_extension'");
    auto thread1 = std::thread{execute, connection1.get()};
    auto thread2 = std::thread{execute, connection2.get()};
    thread1.join();
    thread2.join();
}
