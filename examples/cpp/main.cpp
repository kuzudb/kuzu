#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>

#include "kuzu.hpp"
using namespace kuzu::main;
#include "json.hpp" // Include the nlohmann/json header

using json = nlohmann::json;
using namespace std;

int main() {
    std::filesystem::remove_all("12312312");
    auto database = std::make_unique<Database>("12312312" /* fill db path */);
    auto connection = std::make_unique<Connection>(database.get());
    // Read the JSON file
    ifstream inputFile("/Users/z473chen/Desktop/code/kuzu/examples/cpp/G_47.json");
    if (!inputFile.is_open()) {
        cerr << "Failed to open file." << endl;
        return 1;
    }

    // Parse the JSON file
    json jsonData;
    inputFile >> jsonData;
    inputFile.close();

    // Iterate through each query in the JSON array
    for (const auto& query : jsonData) {
        if (query.is_string()) {
            printf("%s", connection->query(query.get<string>())->toString().c_str());
        } else {
            cerr << "Invalid query format." << endl;
        }
    }

    return 0;
}
