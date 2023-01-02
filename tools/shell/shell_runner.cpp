#include <iostream>

#include "args.hxx"
#include "embedded_shell.h"

using namespace std;
using namespace kuzu::main;

int main(int argc, char* argv[]) {
    args::ArgumentParser parser("KuzuDB Shell");
    args::HelpFlag help(parser, "help", "Display this help menu", {'h', "help"});
    args::ValueFlag<string> inputDirFlag(
        parser, "", "Database path.", {'i', "inputDir"}, args::Options::Required);
    args::ValueFlag<uint64_t> bpSizeInMBFlag(parser, "",
        "Size of buffer pool for default and large page sizes in megabytes", {'d', "defaultBPSize"},
        1024);
    args::Flag inMemoryFlag(parser, "", "Runs the system in in-memory mode", {'m', "in-memory"});
    try {
        parser.ParseCLI(argc, argv);
    } catch (exception& e) {
        cerr << e.what() << std::endl;
        cerr << parser;
        return 1;
    }
    auto databasePath = args::get(inputDirFlag);
    uint64_t bpSizeInMB = args::get(bpSizeInMBFlag);
    cout << "Opened the database at path: " << databasePath << endl;
    cout << "Enter \":help\" for usage hints." << endl;
    SystemConfig systemConfig(bpSizeInMB << 20);
    DatabaseConfig databaseConfig(databasePath, inMemoryFlag);
    auto shell = EmbeddedShell(databaseConfig, systemConfig);
    shell.run();
    return 0;
}
