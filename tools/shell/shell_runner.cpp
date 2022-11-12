#include <iostream>

#include "args.hxx"
#include "tools/shell/include/embedded_shell.h"

using namespace std;

int main(int argc, char* argv[]) {
    args::ArgumentParser parser("KuzuDB Shell");
    args::HelpFlag help(parser, "help", "Display this help menu", {'h', "help"});
    args::ValueFlag<string> inputDirFlag(
        parser, "", "Path to serialized db files.", {'i', "inputDir"}, args::Options::Required);
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
    auto serializedGraphPath = args::get(inputDirFlag);
    uint64_t bpSizeInMB = args::get(bpSizeInMBFlag);
    cout << "serializedGraphPath: " << serializedGraphPath << endl;
    cout << "inMemory: " << (inMemoryFlag ? "true" : "false") << endl;
    cout << "bufferPoolSizeInMB: " << to_string(bpSizeInMB) << endl;
    SystemConfig systemConfig(bpSizeInMB << 20);
    DatabaseConfig databaseConfig(serializedGraphPath, inMemoryFlag);
    auto shell = EmbeddedShell(databaseConfig, systemConfig);
    shell.run();

    return 0;
}
