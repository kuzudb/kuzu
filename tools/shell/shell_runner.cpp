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
        -1u);
    try {
        parser.ParseCLI(argc, argv);
    } catch (exception& e) {
        cerr << e.what() << std::endl;
        cerr << parser;
        return 1;
    }
    auto databasePath = args::get(inputDirFlag);
    cout << "Opened the database at path: " << databasePath << endl;
    cout << "Enter \":help\" for usage hints." << endl;
    uint64_t bpSizeInMB = args::get(bpSizeInMBFlag);
    uint64_t bpSizeInBytes = -1u;
    if (bpSizeInMB != -1u) {
        bpSizeInBytes = bpSizeInMB << 20;
    }
    SystemConfig systemConfig(bpSizeInBytes);
    DatabaseConfig databaseConfig(databasePath);
    try {
        auto shell = EmbeddedShell(databaseConfig, systemConfig);
        shell.run();
    } catch (exception& e) {
        cerr << e.what() << endl;
        return 1;
    }
    return 0;
}
