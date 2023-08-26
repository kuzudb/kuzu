#pragma once

#include "common/data_chunk/data_chunk.h"
#include "common/types/internal_id_t.h"
#include "common/vector/value_vector.h"
#include "serd.h"
#include "spdlog/spdlog.h"

using namespace kuzu::common;

namespace spdlog {
class logger;
}

namespace kuzu {
namespace storage {

class RDFReader {
public:
    explicit RDFReader(std::string filePath, bool strict);

    ~RDFReader();

    offset_t read(const std::shared_ptr<common::ValueVector>& subject,
        const std::shared_ptr<common::ValueVector>& predicate,
        const std::shared_ptr<common::ValueVector>& object);

    inline std::string getFilePath() const { return filePath; }

    inline offset_t getFileSize() const { return fileSize; }

private:
    static SerdStatus errorHandle(void* handle, const SerdError* error);
    static SerdStatus handleStatements(void* handle, SerdStatementFlags flags,
        const SerdNode* graph, const SerdNode* subject, const SerdNode* predicate,
        const SerdNode* object, const SerdNode* object_datatype, const SerdNode* object_lang);
    static bool isSerdTypeSupported(SerdType serdType);

private:
    std::shared_ptr<spdlog::logger> logger;
    const std::string filePath;
    FILE* fp;
    offset_t fileSize;
    SerdReader* reader;
    offset_t numLinesPerBatchRead;
    SerdStatus status;
    // When disabled, this flag helps to skip triples with syntax error (incorrect uri etc.)
    // and continue reading from the next triple. Otherwise, the reader will throw an exception
    // if there's an error in the file.
    bool strict;
    std::shared_ptr<common::ValueVector> subjectVector;
    std::shared_ptr<common::ValueVector> predicateVector;
    std::shared_ptr<common::ValueVector> objectVector;
};

} // namespace storage
} // namespace kuzu
