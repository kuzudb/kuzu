#include <iostream>

#include "args.hxx"
#include "embedded_shell.h"

using namespace kuzu::main;

int main(int argc, char* argv[]) {
    args::ArgumentParser parser("KuzuDB Shell");
    args::Positional<std::string> inputDirFlag(
        parser, "databasePath", "Database path.", args::Options::Required);
    args::HelpFlag help(parser, "help", "Display this help menu", {'h', "help"});
    args::ValueFlag<uint64_t> bpSizeInMBFlag(parser, "",
        "Size of buffer pool for default and large page sizes in megabytes", {'d', "defaultBPSize"},
        -1u);
    args::Flag disableCompression(
        parser, "nocompression", "Disable compression", {"nocompression"});
    args::Flag readOnlyMode(
        parser, "readOnly", "Open database at read-only mode.", {'r', "readOnly"});
    try {
        parser.ParseCLI(argc, argv);
    } catch (std::exception& e) {
        std::cerr << e.what() << std::endl;
        std::cerr << parser;
        return 1;
    }
    auto databasePath = args::get(inputDirFlag);
    uint64_t bpSizeInMB = args::get(bpSizeInMBFlag);
    uint64_t bpSizeInBytes = -1u;
    if (bpSizeInMB != -1u) {
        bpSizeInBytes = bpSizeInMB << 20;
    }
    SystemConfig systemConfig(bpSizeInBytes);
    if (disableCompression) {
        systemConfig.enableCompression = false;
    }
    if (readOnlyMode) {
        systemConfig.readOnly = true;
    }
    std::cout << "Opened the database at path: " << databasePath << " in "
              << (readOnlyMode ? "read-only mode" : "read-write mode") << "." << std::endl;
    std::cout << "Enter \":help\" for usage hints." << std::endl;
    try {
        auto shell = EmbeddedShell(databasePath, systemConfig);
        shell.run();
    } catch (std::exception& e) {
        std::cerr << e.what() << std::endl;
        return 1;
    }
    return 0;
}
