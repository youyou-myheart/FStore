#include "chunk_state.hpp"
#include <array>
#include <cerrno>
#include <system_error>
#include <sys/xattr.h>

namespace chunkstore {
namespace {
State decode(ssize_t count, const char* data) {
    if (count < 0) {
        if (errno == ENODATA) return State::Frozen;
        throw std::system_error(errno, std::generic_category(), "read chunk close reason");
    }
    const std::string_view reason(data, count);
    if (reason == "USER_FREEZE") return State::Frozen;
    if (reason == "PG_REPAIR") return State::Sealed;
    throw std::system_error(EIO, std::generic_category(), "invalid chunk close reason");
}
}
State load_closed_state(int fd) {
    std::array<char, 32> data;
    const auto size = ::fgetxattr(fd, close_reason_attribute, data.data(), data.size());
    return decode(size, data.data());
}
State load_closed_state(const std::filesystem::path& path) {
    std::array<char, 32> data;
    const auto size = ::lgetxattr(path.c_str(), close_reason_attribute, data.data(), data.size());
    return decode(size, data.data());
}
void save_closed_state(int fd, State state) {
    if (!is_closed(state)) throw std::invalid_argument("close reason requires an immutable state");
    const std::string_view reason = state == State::Frozen ? "USER_FREEZE" : "PG_REPAIR";
    if (::fsetxattr(fd, close_reason_attribute, reason.data(), reason.size(), 0) < 0) {
        throw std::system_error(errno, std::generic_category(), "write chunk close reason");
    }
}
}
