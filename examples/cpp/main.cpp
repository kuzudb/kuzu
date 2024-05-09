#include <iostream>

#include "kuzu.hpp"
#include <sys/fcntl.h>

using namespace kuzu::main;

//static void testTime(uint64_t size, std::string path, kuzu::common::VirtualFileSystem* vfs) {
//    auto fileInfo = vfs->openFile(path, O_RDONLY);
//    auto buffer = std::make_unique<uint8_t[]>(size);
//    uint64_t bytesRead;
//    auto start = std::chrono::steady_clock::now();
//    do {
//        bytesRead = fileInfo->readFile(buffer.get(), size);
//    } while (bytesRead > 0);
//    auto end = std::chrono::steady_clock::now();
//    auto duration = duration_cast<std::chrono::seconds>(end - start);
//    printf("Finished, time: %lld. Size: %lld.\n", duration.count(), size);
//}

//int main() {
//    auto database = std::make_unique<Database>("123.db" /* fill db path */);
//    auto connection = std::make_unique<Connection>(database.get());
//
//    printf("%s", connection
//                     ->query("load extension "
//                             "'/Users/z473chen/Desktop/code/kuzu/dataset/"
//                             "libhttpfs.kuzu_extension'")
//                     ->toString()
//                     .c_str());
//    auto vfs = database->getVFS();
//    auto size = 64 * 1024;
//    auto path = "/Users/z473chen/Desktop/code/kuzu/dataset/comment_0_0.csv";
//    testTime(4 * 1024, path, vfs);
//    testTime(64 * 1024, path, vfs);
//    testTime(256 * 1024, path, vfs);
//    testTime(512 * 1024, path, vfs);
//    testTime(1024 * 1024, path, vfs);
//    testTime(4096 * 1024, path, vfs);
//}
