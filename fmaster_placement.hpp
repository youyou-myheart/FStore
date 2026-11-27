#pragma once

#include "fmaster_store.hpp"
#include "chunk_id.hpp"
#include <optional>

namespace chunkstore::master::placement {
inline constexpr std::uint32_t pg_count = global_pg_count;
void normalize_spec(wire::SpaceSpec& spec);
void validate_catalog(const wire::SpaceCatalog& catalog);
void validate_topology(const wire::SpaceCatalog& catalog, const wire::Topology& topology);
wire::PlacementGroup group(const wire::SpaceState& state, std::uint32_t pg);
std::string chunk_id(std::uint32_t space, std::uint64_t sequence, std::uint64_t index);
struct DecodedId { std::uint32_t space; std::uint64_t sequence; std::uint64_t index; std::uint32_t pg; };
DecodedId decode_id(std::string_view id);
bool owns_pg(const wire::SpaceState& state, std::uint32_t pg);
std::uint64_t initial_cursor(const wire::SpaceState& state, std::uint64_t sequence);
std::optional<std::uint64_t> next_cursor(const wire::SpaceState& state, std::uint64_t cursor);
bool owns_blob(const wire::SpaceState& state, std::uint64_t sequence);
} // namespace chunkstore::master::placement
