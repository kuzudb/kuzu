#include <fstream>
#include <iostream>

#include "boost/program_options.hpp"
#include <bits/unique_ptr.h>

#include "src/storage/include/graph_loader.h"

namespace po = boost::program_options;

po::options_description *getOptionDescription() {
    auto description =
        new po::options_description("Loads the csv files as a graph and serializes it.");
    description->add_options()("help", "produce help message")("input_directory,i",
        po::value<std::string>(), "directory to read metadata and input csv files.")(
        "output_directory,o", po::value<std::string>(), "directory to keep serialized data.");
    return description;
}

int main(int argc, char *argv[]) {
    auto description = std::unique_ptr<po::options_description>(getOptionDescription());
    po::variables_map varMap;
    store(parse_command_line(argc, argv, *description), varMap);
    notify(varMap);
    if (varMap.count("help")) {
        std::cout << *description << std::endl;
        return 1;
    }
    if (varMap.size() != 2) {
        std::cout << "Required input and output directory" << std::endl;
        return 1;
    }
    auto graphLoader = std::unique_ptr<graphflow::storage::GraphLoader>(
        new graphflow::storage::GraphLoader(varMap["input_directory"].as<std::string>()));
    auto graph = graphLoader->loadGraph();
}
