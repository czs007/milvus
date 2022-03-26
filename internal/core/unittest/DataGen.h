// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#pragma once

#include <boost/algorithm/string/predicate.hpp>
#include <cstring>
#include <memory>
#include <random>
#include <knowhere/index/vector_index/VecIndex.h>
#include <knowhere/index/vector_index/adapter/VectorAdapter.h>
#include <knowhere/index/vector_index/VecIndexFactory.h>
#include <knowhere/index/vector_index/IndexIVF.h>

#include "Constants.h"
#include "common/Schema.h"
#include "query/SearchOnIndex.h"
#include "segcore/SegmentGrowingImpl.h"
#include "segcore/SegmentSealedImpl.h"

//namespace ser = milvus::proto::milvus;

namespace milvus::test {
using milvus::segcore::SegmentSealed;
using milvus::segcore::RowBasedRawData;
using milvus::proto::milvus::PlaceholderGroup;

struct GeneratedData {
    std::vector<uint8_t> rows_;
    std::vector<aligned_vector<uint8_t>> cols_;
    std::vector<idx_t> row_ids_;
    std::vector<Timestamp> timestamps_;

    RowBasedRawData raw_;

    template <typename T>
    std::vector<T>
    get_col(int index) const {
        auto& target = cols_.at(index);
        std::vector<T> ret(target.size() / sizeof(T));
        memcpy(ret.data(), target.data(), target.size());
        return ret;
    }

    template <typename T>
    T *
    get_mutable_col(int index) {
        auto& target = cols_.at(index);
        assert(target.size() == row_ids_.size() * sizeof(T));
        auto ptr = reinterpret_cast<T*>(target.data());
        return ptr;
    }

 private:
    GeneratedData() = default;
    friend 
    GeneratedData DataGen(SchemaPtr schema, int64_t N, uint64_t seed, uint64_t ts_offset);
    void
    generate_rows(int64_t N, SchemaPtr schema);
};

GeneratedData
DataGen(SchemaPtr schema, int64_t N, uint64_t seed = 42, uint64_t ts_offset = 0);

PlaceholderGroup
CreatePlaceholderGroup(int64_t num_queries, int dim, int64_t seed = 42);

PlaceholderGroup
CreatePlaceholderGroupFromBlob(int64_t num_queries, int dim, const float* src);

PlaceholderGroup
CreateBinaryPlaceholderGroup(int64_t num_queries, int64_t dim, int64_t seed = 42);

PlaceholderGroup
CreateBinaryPlaceholderGroupFromBlob(int64_t num_queries, int64_t dim, const uint8_t* ptr);

json
SearchResultToJson(const SearchResult& sr);

void
SealedLoader(const GeneratedData& dataset, SegmentSealed& seg);

std::unique_ptr<SegmentSealed>
SealedCreator(SchemaPtr schema, const GeneratedData& dataset, const LoadIndexInfo& index_info);

knowhere::VecIndexPtr
GenIndexing(int64_t N, int64_t dim, const float* vec);

}  // namespace milvus::test
