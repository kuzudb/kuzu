#pragma once

#include "common/copier_config/reader_config.h"
#include "common/data_chunk/data_chunk.h"
#include "serd.h"
#include "triple_store.h"

namespace kuzu {
namespace processor {

class RdfReader {
public:
    RdfReader(std::string filePath, common::FileType fileType, RdfStore* store_)
        : RdfReader{std::move(filePath), fileType, store_, 0} {}

    virtual ~RdfReader();

    virtual void init() = 0;

    common::offset_t readChunk(common::DataChunk* dataChunk);
    void readAll();

    std::string getAsString(const SerdNode* node);

    inline uint64_t getNumLiteralTriplesScanned() const { return numLiteralTriplesScanned; }

protected:
    RdfReader(std::string filePath, common::FileType fileType, RdfStore* store_,
        const common::offset_t startOffset)
        : store_{store_}, cursor{0}, startOffset{startOffset}, numLiteralTriplesScanned{0},
          filePath{std::move(filePath)}, fileType{fileType}, reader{nullptr}, status{SERD_SUCCESS} {
    }

    void initInternal(SerdStatementSink statementHandle);

    virtual uint64_t readToVector(common::DataChunk* dataChunk) = 0;

    static SerdStatus errorHandle(void* handle, const SerdError* error);
    static SerdStatus baseHandle(void* handle, const SerdNode* baseNode);
    static SerdStatus prefixHandle(void* handle, const SerdNode* nameNode, const SerdNode* uriNode);

public:
    RdfStore* store_;

protected:
    uint64_t cursor = 0;
    // We use row offset as the primary key for rdf literal. During rel table insertion, we use row
    // offset directly and save the primary key look up. To do so, we need to record how many
    // literal triples have been scanned from previous files (startOffset) and in current file
    // (numLiteralTriplesScanned).
    const common::offset_t startOffset;
    uint64_t numLiteralTriplesScanned;

private:
    std::string filePath;
    common::FileType fileType;

    FILE* fp;
    SerdReader* reader;
    bool hasBaseUri = false;
    SerdEnv* env;

    SerdStatus status;
};

class RdfResourceReader final : public RdfReader {
public:
    RdfResourceReader(std::string filePath, common::FileType fileType, RdfStore* store_)
        : RdfReader{filePath, fileType, store_} {}

    inline void init() override { initInternal(handle); }

private:
    static SerdStatus handle(void* handle, SerdStatementFlags flags, const SerdNode* graph,
        const SerdNode* subject, const SerdNode* predicate, const SerdNode* object,
        const SerdNode* object_datatype, const SerdNode* object_lang);

    uint64_t readToVector(common::DataChunk* dataChunk) override;
};

class RdfLiteralReader final : public RdfReader {
public:
    RdfLiteralReader(std::string filePath, common::FileType fileType, RdfStore* store_)
        : RdfReader{filePath, fileType, store_} {}

    inline void init() override { initInternal(handle); }

private:
    static SerdStatus handle(void* handle, SerdStatementFlags flags, const SerdNode* graph,
        const SerdNode* subject, const SerdNode* predicate, const SerdNode* object,
        const SerdNode* object_datatype, const SerdNode* object_lang);

    uint64_t readToVector(common::DataChunk* dataChunk) override;
};

class RdfResourceTripleReader final : public RdfReader {
public:
    RdfResourceTripleReader(std::string filePath, common::FileType fileType, RdfStore* store_)
        : RdfReader{filePath, fileType, store_} {}

    inline void init() override { initInternal(handle); }

private:
    static SerdStatus handle(void* handle, SerdStatementFlags flags, const SerdNode* graph,
        const SerdNode* subject, const SerdNode* predicate, const SerdNode* object,
        const SerdNode* object_datatype, const SerdNode* object_lang);

    uint64_t readToVector(common::DataChunk* dataChunk) override;
};

class RdfLiteralTripleReader final : public RdfReader {
public:
    RdfLiteralTripleReader(std::string filePath, common::FileType fileType, RdfStore* store_,
        const common::offset_t startOffset)
        : RdfReader{filePath, fileType, store_, startOffset} {}

    inline void init() override { initInternal(handle); }

private:
    static SerdStatus handle(void* handle, SerdStatementFlags flags, const SerdNode* graph,
        const SerdNode* subject, const SerdNode* predicate, const SerdNode* object,
        const SerdNode* object_datatype, const SerdNode* object_lang);

    uint64_t readToVector(common::DataChunk* dataChunk) override;
};

class RdfTripleReader final : public RdfReader {
public:
    RdfTripleReader(std::string filePath, common::FileType fileType, RdfStore* store_)
        : RdfReader{filePath, fileType, store_} {}

    inline void init() override { initInternal(handle); }

private:
    static SerdStatus handle(void* handle, SerdStatementFlags flags, const SerdNode* graph,
        const SerdNode* subject, const SerdNode* predicate, const SerdNode* object,
        const SerdNode* object_datatype, const SerdNode* object_lang);

    uint64_t readToVector(common::DataChunk*) override { KU_UNREACHABLE; }
};

} // namespace processor
} // namespace kuzu
