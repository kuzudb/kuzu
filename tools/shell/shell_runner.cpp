#include <iostream>

#include "args.hxx"
#include "common/file_system/local_file_system.h"
#include "common/task_system/progress_bar.h"
#include "embedded_shell.h"
#include "linenoise.h"
#include "main/db_config.h"
#include "printer/printer_factory.h"

using namespace kuzu::main;
using namespace kuzu::common;

int setConfigOutputMode(const std::string& mode, ShellConfig& shell) {
    shell.printer = PrinterFactory::getPrinter(PrinterTypeUtils::fromString(mode));
    if (shell.printer == nullptr) {
        std::cerr << "Cannot parse '" << mode << "' as output mode." << '\n';
        return 1;
    }
    shell.stats = shell.printer->defaultPrintStats();
    return 0;
}

void processRunCommands(EmbeddedShell& shell, const std::string& filename) {
    FILE* fp = fopen(filename.c_str(), "r");
    char buf[LINENOISE_MAX_LINE + 1];
    buf[LINENOISE_MAX_LINE] = '\0';

    if (fp == NULL) {
        if (filename != ".kuzurc") {
            std::cerr << "Cannot open file " << filename << '\n';
        }
        return;
    }

    std::cout << "-- Loading resources from " << filename << '\n';
    while (fgets(buf, LINENOISE_MAX_LINE, fp) != NULL) {
        auto queryResults = shell.processInput(buf);
        for (auto& queryResult : queryResults) {
            if (!queryResult->isSuccess()) {
                shell.printErrorMessage(buf, *queryResult);
            }
        }
    }
    if (fclose(fp) != 0) {
        // continue regardless of error
    }
}

int main(int argc, char* argv[]) {
    args::ArgumentParser parser("KuzuDB Shell");
    args::Positional<std::string> inputDirFlag(parser, "databasePath",
        "Path to the database. If not given or set to \":memory:\", the database will be opened "
        "under in-memory mode.");
    args::HelpFlag help(parser, "help", "Display this help menu", {'h', "help"});
    args::ValueFlag<uint64_t> bpSizeInMBFlag(parser, "",
        "Size of buffer pool for default and large page sizes in megabytes",
        {'d', "default_bp_size", "defaultbpsize"}, -1u);
    args::ValueFlag<uint64_t> maxDBSize(parser, "max_db_size",
        "Maximum size of the database in bytes", {"max_db_size", "maxdbsize"}, -1u);
    args::Flag disableCompression(parser, "no_compression", "Disable compression",
        {"no_compression", "nocompression"});
    args::Flag readOnlyMode(parser, "read_only", "Open database at read-only mode.",
        {'r', "read_only", "readonly"});
    args::ValueFlag<std::string> historyPathFlag(parser, "", "Path to directory for shell history",
        {'p', "path_history"});
    args::Flag version(parser, "version", "Display current database version", {'v', "version"});
    args::ValueFlag<std::string> mode(parser, "", "Set the output mode of the shell",
        {'m', "mode"});
    args::Flag stats(parser, "no_stats", "Disable query stats", {'s', "no_stats", "nostats"});
    args::Flag progress_bar(parser, "no_progress_bar", "Disable query progress bar",
        {'b', "no_progress_bar", "noprogressbar"});
    args::ValueFlag<std::string> init(parser, "", "Path to file with script to run on startup",
        {'i', "init"});

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
    uint64_t maxDBSizeInBytes = args::get(maxDBSize);
    if (maxDBSizeInBytes != -1u) {
        systemConfig.maxDBSize = maxDBSizeInBytes;
    }
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
        std::make_unique<LocalFileSystem>("")->openFile(pathToHistory,
            FileOpenFlags(
                FileFlags::CREATE_IF_NOT_EXISTS | FileFlags::WRITE | FileFlags::READ_ONLY));
    } catch (Exception&) {
        std::cerr << "Invalid path to directory for history file" << '\n';
        return 1;
    }

    ShellConfig shellConfig;
    shellConfig.path_to_history = pathToHistory.c_str();
    if (mode) {
        if (setConfigOutputMode(args::get(mode), shellConfig) != 0) {
            return 1;
        }
    }
    if (stats) {
        shellConfig.stats = false;
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
    if (!progress_bar) {
        conn->getClientContext()->getClientConfigUnsafe()->enableProgressBar = true;
        conn->getClientContext()->getProgressBar()->toggleProgressBarPrinting(true);
    }

    std::string initFile = ".kuzurc";
    if (init) {
        initFile = args::get(init);
    }
    try {
        auto shell = EmbeddedShell(database, conn, shellConfig);
        processRunCommands(shell, initFile);
        if (shellConfig.stats) {
            if (DBConfig::isDBPathInMemory(databasePath)) {
                std::cout << "Opening the database under in-memory mode." << '\n';
            } else {
                std::cout << "Opening the database at path: " << databasePath << " in "
                          << (readOnlyMode ? "read-only mode" : "read-write mode") << "." << '\n';
            }
            std::cout << "Enter \":help\" for usage hints." << '\n' << std::flush;
        }
        shell.run();
    } catch (std::exception& e) {
        std::cerr << e.what() << '\n';
        return 1;
    }
    return 0;
}
