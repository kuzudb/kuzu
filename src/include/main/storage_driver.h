#pragma once

#include <queue>

#include "common/types/types_include.h"
#include "database.h"

namespace kuzu {
namespace storage {
class Column;
}

namespace main {

//// Note: pooling is tricky to figure out "Busy state"
// class ThreadPool {
// public:
//    ThreadPool(size_t numThreads);
//    ~ThreadPool();
//
//    template<typename F, typename... Args>
//    void enqueue(F&& f, Args&&... args) {
//        {
//            std::unique_lock<std::mutex> lock(queueMtx);
//            tasks.emplace([=] { std::invoke(f, args...); });
//        }
//        condition.notify_one();
//    }
//
// private:
//    std::vector<std::thread> threads;
//    std::queue<std::function<void()>> tasks;
//    std::mutex queueMtx;
//    std::condition_variable condition;
//    bool stop = false;
//};

class StorageDriver {
public:
    explicit StorageDriver(Database* database, size_t numThreads = 1);

    ~StorageDriver();

    std::pair<uint8_t*, size_t> scan(const std::string& nodeName,
        const std::string& propertyName, common::offset_t* offsets, size_t size);

private:
    void scanColumn(
        storage::Column* column, common::offset_t* offsets, size_t size, uint8_t* result);

private:
    catalog::Catalog* catalog;
    storage::StorageManager* storageManager;
    size_t numThreads;
};

} // namespace main
} // namespace kuzu
