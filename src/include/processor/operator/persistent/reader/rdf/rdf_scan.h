#pragma once

#include "function/function.h"
#include "function/table/bind_data.h"
#include "function/table/scan_functions.h"
#include "rdf_reader.h"

namespace kuzu {
namespace processor {

struct RdfScanSharedState : public function::ScanSharedState {
    common::RdfReaderConfig rdfConfig;
    std::unique_ptr<RdfReader> reader;
    std::shared_ptr<RdfStore> store;

    RdfScanSharedState(common::ReaderConfig readerConfig, common::RdfReaderConfig rdfConfig,
        std::shared_ptr<RdfStore> store)
        : ScanSharedState{std::move(readerConfig), 0}, rdfConfig{std::move(rdfConfig)},
          store{std::move(store)}, numLiteralTriplesScanned{0} {}
    explicit RdfScanSharedState(common::ReaderConfig readerConfig,
        common::RdfReaderConfig rdfConfig)
        : RdfScanSharedState{std::move(readerConfig), std::move(rdfConfig), nullptr} {}

    void read(common::DataChunk& dataChunk);
    void readAll();

    void initReader();

private:
    virtual void createReader(uint32_t fileIdx, const std::string& path,
        common::offset_t startOffset) = 0;

    common::offset_t numLiteralTriplesScanned;
};

struct RdfResourceScanSharedState final : public RdfScanSharedState {

    explicit RdfResourceScanSharedState(common::ReaderConfig readerConfig,
        common::RdfReaderConfig rdfConfig)
        : RdfScanSharedState{std::move(readerConfig), std::move(rdfConfig)} {
        KU_ASSERT(store == nullptr);
        store = std::make_shared<ResourceStore>();
        initReader();
    }

    inline void createReader(uint32_t fileIdx, const std::string& path, common::offset_t) override {
        reader = std::make_unique<RdfResourceReader>(rdfConfig, fileIdx, path,
            readerConfig.fileType, store.get());
    }
};

struct RdfLiteralScanSharedState final : public RdfScanSharedState {

    explicit RdfLiteralScanSharedState(common::ReaderConfig readerConfig,
        common::RdfReaderConfig rdfConfig)
        : RdfScanSharedState{std::move(readerConfig), std::move(rdfConfig)} {
        KU_ASSERT(store == nullptr);
        store = std::make_shared<LiteralStore>();
        initReader();
    }

    inline void createReader(uint32_t fileIdx, const std::string& path, common::offset_t) override {
        reader = std::make_unique<RdfLiteralReader>(rdfConfig, fileIdx, path, readerConfig.fileType,
            store.get());
    }
};

struct RdfResourceTripleScanSharedState final : public RdfScanSharedState {

    explicit RdfResourceTripleScanSharedState(common::ReaderConfig readerConfig,
        common::RdfReaderConfig rdfConfig)
        : RdfScanSharedState{std::move(readerConfig), std::move(rdfConfig)} {
        KU_ASSERT(store == nullptr);
        store = std::make_shared<ResourceTripleStore>();
        initReader();
    }

    inline void createReader(uint32_t fileIdx, const std::string& path, common::offset_t) override {
        reader = std::make_unique<RdfResourceTripleReader>(rdfConfig, fileIdx, path,
            readerConfig.fileType, store.get());
    }
};

struct RdfLiteralTripleScanSharedState final : public RdfScanSharedState {

    explicit RdfLiteralTripleScanSharedState(common::ReaderConfig readerConfig,
        common::RdfReaderConfig rdfConfig)
        : RdfScanSharedState{std::move(readerConfig), std::move(rdfConfig)} {
        KU_ASSERT(store == nullptr);
        store = std::make_shared<LiteralTripleStore>();
        initReader();
    }

    void createReader(uint32_t fileIdx, const std::string& path,
        common::offset_t startOffset) override {
        reader = std::make_unique<RdfLiteralTripleReader>(rdfConfig, fileIdx, path,
            readerConfig.fileType, store.get(), startOffset);
    }
};

struct RdfTripleScanSharedState final : public RdfScanSharedState {

    explicit RdfTripleScanSharedState(common::ReaderConfig readerConfig,
        common::RdfReaderConfig rdfConfig, std::shared_ptr<RdfStore> store)
        : RdfScanSharedState{std::move(readerConfig), std::move(rdfConfig), std::move(store)} {
        initReader();
    }

    void createReader(uint32_t fileIdx, const std::string& path, common::offset_t) override {
        reader = std::make_unique<RdfTripleReader>(rdfConfig, fileIdx, path, readerConfig.fileType,
            store.get());
    }
};

struct RdfScanBindData final : public function::ScanBindData {
    std::shared_ptr<RdfStore> store;

    RdfScanBindData(std::vector<common::LogicalType> columnTypes,
        std::vector<std::string> columnNames, common::ReaderConfig config,
        main::ClientContext* context, std::shared_ptr<RdfStore> store)
        : function::ScanBindData{std::move(columnTypes), std::move(columnNames), std::move(config),
              context},
          store{std::move(store)} {}
    RdfScanBindData(const RdfScanBindData& other)
        : function::ScanBindData{other}, store{other.store} {}

    inline std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<RdfScanBindData>(*this);
    }
};

struct RdfInMemScanSharedState : public function::BaseFileScanSharedState {
    std::shared_ptr<RdfStore> store;
    uint64_t rtCursor = 0;
    uint64_t ltCursor = 0;
    // Each triple can be read as at most 3 rows (resources). For simplicity, we read 500 triples
    // per batch to avoid exceeding DEFAULT_VECTOR_CAPACITY.
    static constexpr uint64_t batchSize = 500;

    explicit RdfInMemScanSharedState(std::shared_ptr<RdfStore> store)
        : function::BaseFileScanSharedState{0}, store{std::move(store)} {}

    std::pair<uint64_t, uint64_t> getResourceTripleRange() {
        auto& store_ = common::ku_dynamic_cast<RdfStore&, TripleStore&>(*store);
        return getRange(store_.rtStore, rtCursor);
    }
    std::pair<uint64_t, uint64_t> getLiteralTripleRange() {
        auto& store_ = common::ku_dynamic_cast<RdfStore&, TripleStore&>(*store);
        return getRange(store_.ltStore, ltCursor);
    }

private:
    std::pair<uint64_t, uint64_t> getRange(const RdfStore& store_, uint64_t& cursor);
};

struct RdfResourceScan {
    static constexpr const char* name = "READ_RDF_RESOURCE";

    static function::function_set getFunctionSet();
};

struct RdfLiteralScan {
    static constexpr const char* name = "READ_RDF_LITERAL";

    static function::function_set getFunctionSet();
};

struct RdfResourceTripleScan {
    static constexpr const char* name = "READ_RDF_RESOURCE_TRIPLE";

    static function::function_set getFunctionSet();
};

struct RdfLiteralTripleScan {
    static constexpr const char* name = "READ_RDF_LITERAL_TRIPLE";

    static function::function_set getFunctionSet();
};

struct RdfAllTripleScan {
    static constexpr const char* name = "READ_RDF_ALL_TRIPLE";

    static function::function_set getFunctionSet();
};

struct RdfResourceInMemScan {
    static constexpr const char* name = "IN_MEM_READ_RDF_RESOURCE";

    static function::function_set getFunctionSet();
};

struct RdfLiteralInMemScan {
    static constexpr const char* name = "IN_MEM_READ_RDF_LITERAL";

    static function::function_set getFunctionSet();
};

struct RdfResourceTripleInMemScan {
    static constexpr const char* name = "IN_MEM_READ_RDF_RESOURCE_TRIPLE";

    static function::function_set getFunctionSet();
};

struct RdfLiteralTripleInMemScan {
    static constexpr const char* name = "IN_MEM_READ_RDF_LITERAL_TRIPLE";

    static function::function_set getFunctionSet();
};

} // namespace processor
} // namespace kuzu
