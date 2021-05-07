#pragma once

#include "src/common/include/data_chunk/data_chunk.h"

using namespace std;
using namespace graphflow::common;

namespace graphflow {
namespace transaction {

class LocalStorage {

private:
    vector<unique_ptr<DataChunk>> dataChunks;
};

} // namespace transaction
} // namespace graphflow
