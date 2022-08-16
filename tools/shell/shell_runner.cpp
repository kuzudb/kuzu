#include <iostream>

#include "args.hxx"
#include "tools/shell/include/embedded_shell.h"

using namespace std;

int main(int argc, char* argv[]) {
    args::ArgumentParser parser("GraphflowDB Shell");
    args::HelpFlag help(parser, "help", "Display this help menu", {'h', "help"});
    args::ValueFlag<string> inputDirFlag(
        parser, "", "Path to serialized db files.", {'i', "inputDir"}, args::Options::Required);
    // The default size of buffer pool for holding default page sizes is 1 GB
    args::ValueFlag<uint64_t> defaultPagedBPSizeInMBFlag(parser, "",
        "Size of default paged buffer manager in megabytes", {'d', "defaultPageBPSize"}, 4096);
    // The default size of buffer pool for holding default page sizes is 512 MB
    args::ValueFlag<uint64_t> largePagedBPSizeInMBFlag(parser, "",
        "Size of large paged buffer manager in megabytes", {'l', "largePageBPSize"}, 4096);
    args::Flag inMemoryFlag(parser, "", "Runs the system in in-memory mode", {'m', "in-memory"});
    try {
        parser.ParseCLI(argc, argv);
    } catch (exception& e) {
        cerr << e.what() << std::endl;
        cerr << parser;
        return 1;
    }
    auto serializedGraphPath = args::get(inputDirFlag);
    uint64_t defaultPagedBPSizeInMB = args::get(defaultPagedBPSizeInMBFlag);
    uint64_t largePagedBPSizeInMB = args::get(largePagedBPSizeInMBFlag);
    cout << "serializedGraphPath: " << serializedGraphPath << endl;
    cout << "inMemory: " << (inMemoryFlag ? "true" : "false") << endl;
    cout << "defaultPagedBPSizeInMB: " << to_string(defaultPagedBPSizeInMB) << endl;
    cout << "largePagedBPSizeInMB: " << to_string(largePagedBPSizeInMB) << endl;
    SystemConfig systemConfig(defaultPagedBPSizeInMB << 20, largePagedBPSizeInMB << 20);
    DatabaseConfig databaseConfig(serializedGraphPath, inMemoryFlag);
    auto shell = EmbeddedShell(databaseConfig, systemConfig);
    shell.run();

    return 0;
}
