#include <fcntl.h>

#include <iostream>

#include "args.hxx"
#include "common/file_system/local_file_system.h"
#include "embedded_shell.h"
#include "main/db_config.h"

using namespace kuzu::main;
using namespace kuzu::common;

int main(int argc, char* argv[]) {
    args::ArgumentParser parser("KuzuDB Shell");
    args::Positional<std::string> inputDirFlag(parser, "databasePath",
        "Path to the database. If not given or set to \":memory:\", the database will be opened "
        "under in-memory mode.");
    args::HelpFlag help(parser, "help", "Display this help menu", {'h', "help"});
    args::ValueFlag<uint64_t> bpSizeInMBFlag(parser, "",
        "Size of buffer pool for default and large page sizes in megabytes",
        {'d', "default_bp_size", "defaultbpsize"}, -1u);
    args::Flag disableCompression(parser, "no_compression", "Disable compression",
        {"no_compression", "nocompression"});
    args::Flag readOnlyMode(parser, "read_only", "Open database at read-only mode.",
        {'r', "read_only", "readonly"});
    args::ValueFlag<std::string> historyPathFlag(parser, "", "Path to directory for shell history",
        {'p', "path_history"});
    args::Flag version(parser, "version", "Display current database version", {'v', "version"});

    std::vector<std::string> lCaseArgsStrings;
    for (auto i = 0; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg.size() > 1 && arg[0] == '-') {
            std::string lArg = arg;
            std::transform(lArg.begin(), lArg.end(), lArg.begin(),
                [](unsigned char c) { return std::tolower(c); });
            lCaseArgsStrings.push_back(lArg);
        } else {
            lCaseArgsStrings.push_back(argv[i]);
        }
    }
    std::vector<char*> lCaseArgs;
    for (auto& arg : lCaseArgsStrings) {
        lCaseArgs.push_back(const_cast<char*>(arg.c_str()));
    }

    try {
        parser.ParseCLI(lCaseArgs.size(), lCaseArgs.data());
    } catch (const args::Help&) {
        std::cout << parser;
        return 0;
    } catch (const args::ParseError& e) {
        std::cerr << e.what() << '\n';
        std::cerr << parser;
        return 1;
    }

    if (version) {
        std::cout << "Kuzu " << KUZU_CMAKE_VERSION << '\n';
        return 0;
    }

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

    auto pathToHistory = args::get(historyPathFlag);
    if (!pathToHistory.empty() && pathToHistory.back() != '/') {
        pathToHistory += '/';
    }
    pathToHistory += "history.txt";
    try {
        std::make_unique<LocalFileSystem>()->openFile(pathToHistory, O_CREAT);
    } catch (Exception& e) {
        std::cerr << "Invalid path to directory for history file" << '\n';
        return 1;
    }

    auto databasePath = args::get(inputDirFlag);
    std::shared_ptr<Database> database;
    std::shared_ptr<Connection> conn;
    try {
        database = std::make_shared<Database>(databasePath, systemConfig);
        conn = std::make_shared<Connection>(database.get());
    } catch (Exception& e) {
        std::cerr << e.what() << '\n';
        return 1;
    }
    if (DBConfig::isDBPathInMemory(databasePath)) {
        std::cout << "Opened the database under in in-memory mode." << '\n';
    } else {
        std::cout << "Opened the database at path: " << databasePath << " in "
                  << (readOnlyMode ? "read-only mode" : "read-write mode") << "." << '\n';
    }
    std::cout << "Enter \":help\" for usage hints." << '\n' << std::flush;
    try {
        auto shell = EmbeddedShell(database, conn, pathToHistory.c_str());
        shell.run();
    } catch (std::exception& e) {
        std::cerr << e.what() << '\n';
        return 1;
    }
    return 0;
}
