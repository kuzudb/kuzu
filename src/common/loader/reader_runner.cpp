#include <iostream>

#include "src/common/loader/include/csv_reader.h"

using namespace std;
using namespace graphflow::common;

int main() {
    auto path = "/home/p43gupta/datasets/ldbc/0.1/csv/v-tag.csv";
    auto reader = CSVReader(path, ',', 0);

    while (reader.hasMore()) {
        cout << *(reader.getLine()) << '\n';
    }
}