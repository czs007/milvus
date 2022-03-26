# Copyright (C) 2019-2020 Zilliz. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied. See the License for the specific language governing permissions and limitations under the License

include_directories(${CMAKE_HOME_DIRECTORY}/src)
include_directories(${CMAKE_HOME_DIRECTORY}/src/thirdparty)

add_definitions(-DMILVUS_TEST_SEGCORE_YAML_PATH="${CMAKE_SOURCE_DIR}/unittest/test_segcore.yaml")

if (LINUX)
    # TODO: better to use ls/find pattern

    set(MILVUS_TEST_UTILS_FILES DataGen.cpp
	IndexBuilder.cpp
	    )

    file(GLOB MILVUS_TEST_FILES
		"test_*.cpp"
	    )

    file(GLOB MILVUS_BENCH_FILES
		"bench_*.cpp"
	    )

    set(MILVUS_TEST_FILES2
            init_gtest.cpp
            test_binary.cpp
            test_bitmap.cpp
            test_common.cpp
            test_concurrent_vector.cpp
            test_c_api.cpp
            test_expr.cpp
            test_get_entity_by_ids.cpp
            test_indexing.cpp
            test_index_wrapper.cpp
            test_init.cpp
            test_plan_proto.cpp
            test_query.cpp
            test_reduce.cpp
            test_sealed.cpp
            test_segcore.cpp
            test_span.cpp
            test_timestamp_index.cpp
            test_reduce_c.cpp
            test_conf_adapter_mgr.cpp
            test_similarity_corelation.cpp
            test_utils.cpp
            test_scalar_index_creator.cpp
            test_index_c_api.cpp
            test_index.cpp
            ${MILVUS_TEST_UTILS_FILES}
            )
endif()

if ( APPLE )
    set(MILVUS_TEST_FILES
            init_gtest.cpp
            test_common.cpp
            test_concurrent_vector.cpp
            test_conf_adapter_mgr.cpp
            test_plan_proto.cpp
            test_reduce.cpp
            test_reduce_c.cpp
            test_segcore.cpp
            test_timestamp_index.cpp
            test_similarity_corelation.cpp
            test_utils.cpp
            ${MILVUS_TEST_UTILS_FILES}
            )
endif ()

message("fiels" ${MILVUS_TEST_UTILS_FILES})

add_executable(all_tests init_gtest.cpp ${MILVUS_TEST_FILES}
	${MILVUS_TEST_UTILS_FILES}
        )

target_link_libraries(all_tests
        gtest
    	gtest_main
        milvus_segcore
        milvus_indexbuilder
        milvus_index
        log
        pthread
        )

install(TARGETS all_tests DESTINATION unittest)

add_executable(all_bench ${MILVUS_TEST_UTILS_FILES}
	${MILVUS_BENCH_FILES})

target_link_libraries(all_bench
	benchmark::benchmark_main
        milvus_segcore
	milvus_indexbuilder
        milvus_index
        log
        pthread
        )
install(TARGETS all_bench DESTINATION unittest)
