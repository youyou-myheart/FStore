#include "chunk_store.hpp"

#include <array>
#include <barrier>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <thread>
#include <unistd.h>
#include <vector>

namespace fs = std::filesystem;
using namespace chunkstore;

namespace {
void check(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}

void verify(const fs::path& directory) {
    ChunkStore store(directory);
    const std::string id(32, 'a');
    const std::string a(256 * 1024, 'a'), b(a.size(), 'b');
    store.create_chunk(id, a.size());
    store.append(id, a);
    store.freeze_chunk(id);
    check(store.read_chunk(id, 0, a.size()) == a, "initial read failed");
    const auto timestamp = store.delete_chunk(id);
    try {
        store.read_chunk(id, 0, 1);
        throw std::runtime_error("retired version readable after delete");
    } catch (const std::system_error& e) {
        if (e.code() != std::errc::no_such_file_or_directory) throw;
    }
    store.create_chunk(id, a.size());
    store.append(id, b);
    check(store.read_chunk(id, 0, b.size()) == b, "cached fd survived ID reuse");
    store.delete_chunk(id);
    store.restore_chunk(id, timestamp);
    check(store.read_chunk(id, 0, a.size()) == a, "cached fd survived restore");

    constexpr int reader_count = 6;
    std::barrier start(reader_count + 1);
    std::array<std::exception_ptr, reader_count> errors;
    std::vector<std::jthread> readers;
    for (auto& error : errors) {
        readers.emplace_back([&](std::stop_token stop) {
            start.arrive_and_wait();
            try {
                do {
                    auto content = store.read_chunk(id, 0, a.size());
                    check(content == a || content == b, "mixed file generations");
                } while (!stop.stop_requested());
            } catch (...) { error = std::current_exception(); }
        });
    }
    start.arrive_and_wait();
    for (unsigned i = 0; i < 40; ++i) {
        const auto& value = i % 2 == 0 ? b : a;
        const int fd = store.open_repair_file(id);
        try {
            check(::write(fd, value.data(), value.size()) == static_cast<ssize_t>(value.size()), "write repair file");
            store.install_replica(id, fd, {value.size(), value.size(), State::Frozen});
        } catch (...) {
            ::close(fd);
            throw;
        }
        ::close(fd);
        check(store.read_chunk(id, 0, value.size()) == value, "repair did not retire cached version");
    }
    for (auto& reader : readers) reader.request_stop();
    readers.clear();
    for (const auto& error : errors) if (error) std::rethrow_exception(error);
}
}

int main() {
    char directory[] = "/tmp/fstore-read-version-XXXXXX";
    if (!::mkdtemp(directory)) return 1;
    try {
        verify(directory);
    } catch (const std::exception& e) {
        std::cerr << e.what() << '\n';
        fs::remove_all(directory);
        return 1;
    }
    fs::remove_all(directory);
}
