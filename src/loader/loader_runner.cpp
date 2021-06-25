#include <iostream>
#include <unordered_map>

#include "args.hxx"
#include "spdlog/spdlog.h"

#include "src/loader/include/graph_loader.h"

using namespace graphflow::loader;
using namespace std;

void createDirectory(const string& directory) {
    struct stat st;
    if (stat(directory.c_str(), &st) != 0) {
        if (mkdir(directory.c_str(), 0755) != 0 && errno != EEXIST) {
            throw invalid_argument("Failed to create directory" + directory);
        }
    }
}

unordered_map<string, spdlog::level::level_enum> verbosityMap{
    {"trace", spdlog::level::trace},
    {"debug", spdlog::level::debug},
    {"info", spdlog::level::info},
    {"warn", spdlog::level::warn},
    {"critical", spdlog::level::critical},
    {"error", spdlog::level::err},
};

int main(int argc, char* argv[]) {
    args::ArgumentParser parser("Ingest the CSV files and saves in the Graphflow's native format.");
    args::HelpFlag help(parser, "help", "Display this help menu", {'h', "help"});
    args::ValueFlag<int> threads(parser, "threads",
        "Maximum number of parallel threads for data ingestion. Default is set to "
        "thread::hardware_concurrency()",
        {'t', "threads"});
    args::MapFlag<string, spdlog::level::level_enum> verbosity(
        parser, "verbosity", "Sets the verbosity of the logger", {'v', "verbosity"}, verbosityMap);
    args::Positional<std::string> inputDir(
        parser, "inputDir", "Input directory from where metadata and csv files are read.");
    args::Positional<std::string> outputDir(
        parser, "outputDir", "Output directory to save Graphflow's native format.");
    try {
        parser.ParseCLI(argc, argv);
    } catch (const args::Help&) {
        cout << parser;
        return 0;
    } catch (const args::ParseError& e) {
        cerr << e.what() << std::endl;
        cerr << parser;
        return 1;
    } catch (const args::ValidationError& e) {
        cerr << e.what() << std::endl;
        cerr << parser;
        return 1;
    }
    spdlog::set_level(verbosity ? args::get(verbosity) : verbosityMap["info"]);
    createDirectory(args::get(outputDir));
    auto numThreads = threads ? args::get(threads) : thread::hardware_concurrency();
    try {
        GraphLoader graphLoader(args::get(inputDir), args::get(outputDir), numThreads);
        graphLoader.loadGraph();
    } catch (const exception& e) { cerr << "Failed to load graph. Error: " << e.what() << endl; }
    return 0;
}
