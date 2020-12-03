#include <fstream>
#include <iostream>

#include "args.hxx"
#include <bits/unique_ptr.h>

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

int main(int argc, char* argv[]) {
    args::ArgumentParser parser("Ingest the CSV files and saves in the Graphflow's native format.");
    args::HelpFlag help(parser, "help", "Display this help menu", {'h', "help"});
    args::Positional<std::string> inputDir(
        parser, "inputDir", "Input directory from where metadata and csv files are read.");
    args::Positional<std::string> outputDir(
        parser, "outputDir", "Output directory to save Graphflow's native format.");
    args::ValueFlag<int> threads(parser, "threads", "The integer flag", {'t'});
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
    createDirectory(args::get(outputDir));
    auto numThreads = threads ? args::get(threads) : thread::hardware_concurrency();
    GraphLoader graphLoader(args::get(inputDir), args::get(outputDir), numThreads);
    graphLoader.loadGraph();
    return 0;
}
