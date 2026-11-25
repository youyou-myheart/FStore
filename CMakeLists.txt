cmake_minimum_required(VERSION 3.20)
project(chunkstore LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Threads REQUIRED)
find_package(Protobuf REQUIRED)
find_package(PkgConfig REQUIRED)
pkg_check_modules(BRPC REQUIRED IMPORTED_TARGET brpc)

protobuf_generate_cpp(PROTO_SOURCES PROTO_HEADERS chunk.proto)
add_library(chunk_rpc STATIC ${PROTO_SOURCES} ${PROTO_HEADERS})
target_include_directories(chunk_rpc PUBLIC "${CMAKE_CURRENT_BINARY_DIR}")
target_link_libraries(chunk_rpc PUBLIC protobuf::libprotobuf PkgConfig::BRPC Threads::Threads)

protobuf_generate_cpp(MASTER_PROTO_SOURCES MASTER_PROTO_HEADERS fmaster.proto)
add_library(fmaster_rpc STATIC ${MASTER_PROTO_SOURCES} ${MASTER_PROTO_HEADERS})
target_include_directories(fmaster_rpc PUBLIC "${CMAKE_CURRENT_BINARY_DIR}")
target_link_libraries(fmaster_rpc PUBLIC protobuf::libprotobuf PkgConfig::BRPC Threads::Threads)

add_library(fmaster_store STATIC fmaster_store.cpp)
target_include_directories(fmaster_store PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}")
target_link_libraries(fmaster_store PUBLIC fmaster_rpc)

add_library(fmaster_client STATIC fmaster_client.cpp)
target_include_directories(fmaster_client PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}")
target_link_libraries(fmaster_client PUBLIC fmaster_rpc)

add_executable(fmaster fmaster_server.cpp fmaster_monitor.cpp)
target_link_libraries(fmaster PRIVATE fmaster_store)
add_executable(fmaster_cli fmaster_cli.cpp)
target_link_libraries(fmaster_cli PRIVATE fmaster_client)

add_library(chunk_store STATIC chunk_store.cpp traffic_limiter.cpp)
target_include_directories(chunk_store PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}")
target_link_libraries(chunk_store PUBLIC Threads::Threads)

add_library(chunk_client STATIC chunk_client.cpp)
target_include_directories(chunk_client PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}")
target_link_libraries(chunk_client PUBLIC chunk_rpc)

add_executable(chunk_server chunk_server.cpp chunk_repair.cpp fserver_heartbeat.cpp)
target_link_libraries(chunk_server PRIVATE chunk_store chunk_rpc fmaster_rpc)

add_executable(chunk_cli chunk_cli.cpp)
target_link_libraries(chunk_cli PRIVATE chunk_client)

include(CTest)
if(BUILD_TESTING)
    add_executable(fmaster_probe_test tests/fmaster_probe.cpp)
    target_link_libraries(fmaster_probe_test PRIVATE fmaster_rpc)
    add_executable(fmaster_store_test tests/fmaster_store.cpp)
    target_link_libraries(fmaster_store_test PRIVATE fmaster_store)
    add_test(NAME fmaster_store COMMAND fmaster_store_test)
    add_executable(replicated_read_test tests/replicated_read.cpp)
    target_link_libraries(replicated_read_test PRIVATE chunk_client)
    add_executable(replica_reads_test tests/replica_reads.cpp)
    target_link_libraries(replica_reads_test PRIVATE chunk_client)
    add_executable(traffic_limiter_test tests/traffic_limiter.cpp)
    target_link_libraries(traffic_limiter_test PRIVATE chunk_store)
    add_test(NAME traffic_limiter COMMAND traffic_limiter_test)
    add_executable(read_version_test tests/read_version.cpp)
    target_link_libraries(read_version_test PRIVATE chunk_store)
    add_test(NAME read_version COMMAND read_version_test)
    set_tests_properties(fmaster_store traffic_limiter read_version PROPERTIES LABELS "unit" TIMEOUT 30)
    find_package(Python3 QUIET COMPONENTS Interpreter)
    if(Python3_Interpreter_FOUND)
        add_test(NAME fmaster COMMAND "${Python3_EXECUTABLE}" -B
                 "${CMAKE_CURRENT_SOURCE_DIR}/tests/fmaster.py" "$<TARGET_FILE_DIR:fmaster_cli>")
        add_test(NAME fmaster_commands COMMAND "${Python3_EXECUTABLE}" -B
                 "${CMAKE_CURRENT_SOURCE_DIR}/tests/fmaster_commands.py" "$<TARGET_FILE_DIR:fmaster_cli>")
        add_test(NAME replicated_client COMMAND "${Python3_EXECUTABLE}" -B
                 "${CMAKE_CURRENT_SOURCE_DIR}/tests/replicated_client.py" "$<TARGET_FILE_DIR:chunk_cli>")
        add_test(NAME worker_polling COMMAND "${Python3_EXECUTABLE}" -B
                 "${CMAKE_CURRENT_SOURCE_DIR}/tests/polling.py" "$<TARGET_FILE_DIR:chunk_cli>")
        add_test(NAME chunk_repair COMMAND "${Python3_EXECUTABLE}" -B
                 "${CMAKE_CURRENT_SOURCE_DIR}/tests/repair.py" "$<TARGET_FILE_DIR:chunk_cli>")
        add_test(NAME chunk_trash COMMAND "${Python3_EXECUTABLE}" -B
                 "${CMAKE_CURRENT_SOURCE_DIR}/tests/trash.py" "$<TARGET_FILE_DIR:chunk_cli>")
        add_test(NAME traffic_integration COMMAND "${Python3_EXECUTABLE}" -B
                 "${CMAKE_CURRENT_SOURCE_DIR}/tests/traffic.py" "$<TARGET_FILE_DIR:chunk_cli>")
        add_test(NAME onebox COMMAND "${Python3_EXECUTABLE}" -B
                 "${CMAKE_CURRENT_SOURCE_DIR}/tests/onebox.py" "$<TARGET_FILE_DIR:chunk_cli>")
        set_tests_properties(fmaster fmaster_commands replicated_client worker_polling chunk_repair
                             PROPERTIES LABELS "integration" TIMEOUT 120)
        set_tests_properties(chunk_trash traffic_integration
                             PROPERTIES LABELS "integration;slow" TIMEOUT 180)
        set_tests_properties(traffic_integration PROPERTIES RUN_SERIAL TRUE)
        set_tests_properties(onebox PROPERTIES LABELS "e2e" TIMEOUT 120)
    endif()
endif()

add_executable(chunk_bench EXCLUDE_FROM_ALL benchmarks/chunk_bench.cpp)
target_link_libraries(chunk_bench PRIVATE chunk_client)
