#pragma once

#include "common/copier_config/rdf_reader_config.h"
#include "common/copier_config/reader_config.h"
#include "common/data_chunk/data_chunk.h"
#include "serd.h"
#include "triple_store.h"

namespace kuzu {
namespace processor {

class RdfReader {
public:
    RdfReader(common::RdfReaderConfig rdfConfig, uint32_t fileIdx, std::string filePath,
        common::FileType fileType, RdfStore* store_)
        : RdfReader{std::move(rdfConfig), fileIdx, std::move(filePath), fileType, store_, 0} {}

    virtual ~RdfReader();

    virtual void init() = 0;

    common::offset_t readChunk(common::DataChunk* dataChunk);
    void readAll();

    std::string getAsString(const SerdNode* node);

    inline uint64_t getNumLiteralTriplesScanned() const { return numLiteralTriplesScanned; }

protected:
    RdfReader(common::RdfReaderConfig rdfConfig, uint32_t fileIdx, std::string filePath,
        common::FileType fileType, RdfStore* store_, const common::offset_t startOffset)
        : store_{store_}, cursor{0}, startOffset{startOffset}, numLiteralTriplesScanned{0},
          rdfConfig{std::move(rdfConfig)}, fileIdx{fileIdx}, filePath{std::move(filePath)},
          fileType{fileType}, reader{nullptr}, status{SERD_SUCCESS} {}

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
    common::RdfReaderConfig rdfConfig;
    uint32_t fileIdx; // We use fileIdx to differentiate blank nodes in different files.
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
    RdfResourceReader(common::RdfReaderConfig rdfConfig, uint32_t fileIdx, std::string filePath,
        common::FileType fileType, RdfStore* store_)
        : RdfReader{std::move(rdfConfig), fileIdx, filePath, fileType, store_} {}

    inline void init() override { initInternal(handle); }

private:
    static SerdStatus handle(void* handle, SerdStatementFlags flags, const SerdNode* graph,
        const SerdNode* subject, const SerdNode* predicate, const SerdNode* object,
        const SerdNode* object_datatype, const SerdNode* object_lang);

    uint64_t readToVector(common::DataChunk* dataChunk) override;
};

class RdfLiteralReader final : public RdfReader {
public:
    RdfLiteralReader(common::RdfReaderConfig rdfConfig, uint32_t fileIdx, std::string filePath,
        common::FileType fileType, RdfStore* store_)
        : RdfReader{std::move(rdfConfig), fileIdx, filePath, fileType, store_} {}

    inline void init() override { initInternal(handle); }

private:
    static SerdStatus handle(void* handle, SerdStatementFlags flags, const SerdNode* graph,
        const SerdNode* subject, const SerdNode* predicate, const SerdNode* object,
        const SerdNode* object_datatype, const SerdNode* object_lang);

    uint64_t readToVector(common::DataChunk* dataChunk) override;
};

class RdfResourceTripleReader final : public RdfReader {
public:
    RdfResourceTripleReader(common::RdfReaderConfig rdfConfig, uint32_t fileIdx,
        std::string filePath, common::FileType fileType, RdfStore* store_)
        : RdfReader{std::move(rdfConfig), fileIdx, filePath, fileType, store_} {}

    inline void init() override { initInternal(handle); }

private:
    static SerdStatus handle(void* handle, SerdStatementFlags flags, const SerdNode* graph,
        const SerdNode* subject, const SerdNode* predicate, const SerdNode* object,
        const SerdNode* object_datatype, const SerdNode* object_lang);

    uint64_t readToVector(common::DataChunk* dataChunk) override;
};

class RdfLiteralTripleReader final : public RdfReader {
public:
    RdfLiteralTripleReader(common::RdfReaderConfig rdfConfig, uint32_t fileIdx,
        std::string filePath, common::FileType fileType, RdfStore* store_,
        const common::offset_t startOffset)
        : RdfReader{std::move(rdfConfig), fileIdx, filePath, fileType, store_, startOffset} {}

    inline void init() override { initInternal(handle); }

private:
    static SerdStatus handle(void* handle, SerdStatementFlags flags, const SerdNode* graph,
        const SerdNode* subject, const SerdNode* predicate, const SerdNode* object,
        const SerdNode* object_datatype, const SerdNode* object_lang);

    uint64_t readToVector(common::DataChunk* dataChunk) override;
};

class RdfTripleReader final : public RdfReader {
public:
    RdfTripleReader(common::RdfReaderConfig rdfConfig, uint32_t fileIdx, std::string filePath,
        common::FileType fileType, RdfStore* store_)
        : RdfReader{std::move(rdfConfig), fileIdx, filePath, fileType, store_} {}

    inline void init() override { initInternal(handle); }

private:
    static SerdStatus handle(void* handle, SerdStatementFlags flags, const SerdNode* graph,
        const SerdNode* subject, const SerdNode* predicate, const SerdNode* object,
        const SerdNode* object_datatype, const SerdNode* object_lang);

    uint64_t readToVector(common::DataChunk*) override { KU_UNREACHABLE; }
};

} // namespace processor
} // namespace kuzu
