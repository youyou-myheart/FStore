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

add_library(chunk_store STATIC chunk_store.cpp traffic_limiter.cpp)
target_include_directories(chunk_store PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}")
target_link_libraries(chunk_store PUBLIC Threads::Threads)

add_library(chunk_client STATIC chunk_client.cpp)
target_include_directories(chunk_client PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}")
target_link_libraries(chunk_client PUBLIC chunk_rpc)

add_executable(chunk_server chunk_server.cpp chunk_repair.cpp)
target_link_libraries(chunk_server PRIVATE chunk_store chunk_rpc)

add_executable(chunk_cli chunk_cli.cpp)
target_link_libraries(chunk_cli PRIVATE chunk_client)

include(CTest)
if(BUILD_TESTING)
    add_executable(traffic_limiter_test tests/traffic_limiter.cpp)
    target_link_libraries(traffic_limiter_test PRIVATE chunk_store)
    add_test(NAME traffic_limiter COMMAND traffic_limiter_test)
    find_package(Python3 QUIET COMPONENTS Interpreter)
    if(Python3_Interpreter_FOUND)
        add_test(NAME replicated_client COMMAND "${Python3_EXECUTABLE}" -B
                 "${CMAKE_CURRENT_SOURCE_DIR}/tests/replicated_client.py" "$<TARGET_FILE_DIR:chunk_cli>")
    endif()
endif()
