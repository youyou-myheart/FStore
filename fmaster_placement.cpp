#include "fmaster_placement.hpp"
#include "chunk_id.hpp"

#include <algorithm>
#include <limits>
#include <map>
#include <set>
#include <unordered_map>
#include <vector>

namespace chunkstore::master::placement {
namespace {

// Version 1 placement hash. Never use std::hash for persisted placement.
std::uint64_t score(std::uint32_t space, std::uint32_t pg, std::string_view server) {
    std::uint64_t h = 14695981039346656037ULL;
    for (const unsigned char c : server) h = (h ^ c) * 1099511628211ULL;
    h ^= (static_cast<std::uint64_t>(space) << 32) | pg;
    h = (h ^ (h >> 30)) * 0xbf58476d1ce4e5b9ULL;
    h = (h ^ (h >> 27)) * 0x94d049bb133111ebULL;
    return h ^ (h >> 31);
}

void require(bool condition, const char* message) {
    if (!condition) throw Error(wire::INVALID_ARGUMENT, message);
}
}

void normalize_spec(wire::SpaceSpec& spec) {
    require(spec.replicas() >= 1 && spec.replicas() <= 16, "replicas must be in 1..16");
    require(spec.chunk_size() > 0 && spec.chunk_size() <= static_cast<std::uint64_t>(INT64_MAX),
            "chunk size must be in 1..INT64_MAX bytes");
    auto& policy = *spec.mutable_placement_policy();
    require(wire::PlacementPolicy_Mode_IsValid(policy.mode()), "invalid placement policy mode");
    if (!policy.min_datacenters()) {
        policy.set_min_datacenters(policy.mode() == wire::PlacementPolicy::SINGLE_DATACENTER ? 1 :
                                  policy.mode() == wire::PlacementPolicy::DISTINCT_DATACENTERS ? spec.replicas() : 2);
    }
    require(policy.min_datacenters() <= spec.replicas() &&
            (policy.mode() == wire::PlacementPolicy::SINGLE_DATACENTER ? policy.min_datacenters() == 1 :
             policy.mode() == wire::PlacementPolicy::DISTINCT_DATACENTERS ? policy.min_datacenters() == spec.replicas() :
             policy.min_datacenters() >= 2), "invalid minimum datacenters for placement policy");
    std::set<std::string> names;
    for (const auto& dc : policy.datacenters()) {
        require(!dc.empty() && dc.size() <= 64 &&
                dc.find_first_not_of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-") == std::string::npos &&
                names.insert(dc).second, "invalid or duplicate allowed datacenter");
    }
    require(names.empty() || names.size() >= policy.min_datacenters(), "allowed datacenters cannot satisfy placement policy");
    std::sort(policy.mutable_datacenters()->begin(), policy.mutable_datacenters()->end());
}

wire::PlacementGroup group(const wire::SpaceState& state, std::uint32_t pg) {
    require(pg < pg_count, "PG ID must be in 0..9999999");
    wire::PlacementGroup result;
    result.set_id(pg);
    if (!owns_pg(state, pg)) return result;
    result.set_active(true);
    const auto overridden = std::lower_bound(state.overrides().begin(), state.overrides().end(), pg,
        [](const wire::PlacementGroup& value, std::uint32_t id) { return value.id() < id; });
    if (overridden != state.overrides().end() && overridden->id() == pg) return *overridden;

    const auto range = std::lower_bound(state.ranges().begin(), state.ranges().end(), pg,
        [](const wire::PGRange& value, std::uint32_t id) { return value.end() <= id; });
    if (range == state.ranges().end() || pg < range->begin() || range->algorithm_version() != 1) {
        throw Error(wire::STORAGE_ERROR, "missing or unsupported PG placement range");
    }
    struct Candidate { const wire::ReplicaTarget* node; std::uint64_t score; };
    std::vector<Candidate> nodes;
    std::map<std::string, std::size_t> available;
    for (const auto& node : range->candidates()) {
        nodes.push_back({&node, score(state.space().id(), pg, node.server_id())});
        ++available[node.datacenter()];
    }
    std::sort(nodes.begin(), nodes.end(), [](const Candidate& a, const Candidate& b) {
        return a.score != b.score ? a.score > b.score : a.node->server_id() < b.node->server_id();
    });
    const auto& spec = state.space().spec();
    if (spec.placement_policy().mode() == wire::PlacementPolicy::SINGLE_DATACENTER) {
        const auto first = std::find_if(nodes.begin(), nodes.end(), [&](const Candidate& node) {
            return available.at(node.node->datacenter()) >= spec.replicas();
        });
        if (first == nodes.end()) throw Error(wire::CONFLICT, "no datacenter has enough distinct FServers");
        const auto dc = first->node->datacenter();
        for (const auto& node : nodes) {
            if (node.node->datacenter() == dc) *result.add_replicas() = *node.node;
            if (result.replicas_size() == static_cast<int>(spec.replicas())) return result;
        }
    } else {
        if (available.size() < spec.placement_policy().min_datacenters() || nodes.size() < spec.replicas() || available.contains("")) {
            throw Error(wire::CONFLICT, "not enough labeled datacenters or distinct FServers");
        }
        std::map<std::string, unsigned> selected;
        while (result.replicas_size() < static_cast<int>(spec.replicas())) {
            auto best = nodes.begin();
            for (auto node = nodes.begin(); node != nodes.end(); ++node) {
                if (selected[node->node->datacenter()] < selected[best->node->datacenter()]) best = node;
            }
            *result.add_replicas() = *best->node;
            ++selected[best->node->datacenter()];
            nodes.erase(best);
        }
        return result;
    }
    throw Error(wire::CONFLICT, "not enough placement candidates");
}

void validate_catalog(const wire::SpaceCatalog& catalog) {
    require(catalog.revision() != 0 && catalog.next_space_id() <= 256, "invalid space catalog counters");
    std::set<std::string> names;
    std::set<std::uint32_t> ids;
    std::vector<std::pair<std::uint64_t, std::uint64_t>> blob_ranges;
    std::vector<std::pair<std::uint32_t, std::uint32_t>> global_ranges;
    require(catalog.reserved_blob_sequence() <= ChunkId::max_sequence, "Blob sequence exceeds 48 bits");
    for (const auto& state : catalog.spaces()) {
        require(state.reserved_sequence() == 0, "legacy Chunk sequence cannot appear in a 120-bit catalog");
        const auto& space = state.space();
        require(!space.name().empty() && space.name().size() <= 64 &&
                space.name().find_first_not_of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-") == std::string::npos,
                "invalid space name");
        require(names.insert(space.name()).second && ids.insert(space.id()).second &&
                space.id() < catalog.next_space_id(), "invalid or duplicate space identity");
        std::uint64_t previous_sequence = 0;
        for (const auto& range : state.blob_sequences()) {
            require(range.begin() > previous_sequence && range.end() >= range.begin() &&
                    range.end() <= catalog.reserved_blob_sequence(), "invalid Blob sequence reservation");
            previous_sequence = range.end();
            blob_ranges.emplace_back(range.begin(), range.end());
        }
        auto spec = space.spec();
        normalize_spec(spec);
        require(spec.SerializeAsString() == space.spec().SerializeAsString(), "space policy is not normalized");
        require(space.total_pgs() == pg_count && space.bound_pgs() <= pg_count, "invalid PG count");
        std::uint32_t next = 0, bound = 0;
        for (const auto& range : state.ranges()) {
            require(range.begin() >= next && range.end() > range.begin() && range.end() <= pg_count &&
                    range.algorithm_version() == 1, "invalid PG activation ranges");
            std::set<std::string> servers, endpoints;
            for (const auto& node : range.candidates()) {
                require(!node.server_id().empty() && !node.endpoint().empty() &&
                        servers.insert(node.server_id()).second && endpoints.insert(node.endpoint()).second,
                        "invalid or duplicate placement candidate");
                const auto& allowed = spec.placement_policy().datacenters();
                require(allowed.empty() || std::find(allowed.begin(), allowed.end(), node.datacenter()) != allowed.end(),
                        "placement candidate is outside allowed datacenters");
            }
            // Feasibility depends on candidate counts, not on the hash order.
            wire::SpaceState check; *check.mutable_space() = space; *check.add_ranges() = range;
            group(check, range.begin());
            next = range.end();
            bound += range.end() - range.begin();
            global_ranges.emplace_back(range.begin(), range.end());
        }
        require(bound == space.bound_pgs(), "bound PG count is inconsistent");
        std::optional<std::uint32_t> previous_pg;
        for (const auto& pg : state.overrides()) {
            require(pg.active() && owns_pg(state, pg.id()) && (!previous_pg || *previous_pg < pg.id()) &&
                    pg.epoch() <= catalog.recovery_sequence() && pg.replicas_size() == static_cast<int>(spec.replicas()), "invalid PG override");
            wire::SpaceState check; *check.mutable_space() = space;
            auto* pool = check.add_ranges(); pool->set_begin(pg.id()); pool->set_end(pg.id() + 1); pool->set_algorithm_version(1);
            std::set<std::string> servers, endpoints;
            for (const auto& node : pg.replicas()) {
                require(!node.server_id().empty() && !node.endpoint().empty() && servers.insert(node.server_id()).second && endpoints.insert(node.endpoint()).second,
                        "duplicate override replica");
                const auto& allowed = spec.placement_policy().datacenters();
                require(allowed.empty() || std::find(allowed.begin(), allowed.end(), node.datacenter()) != allowed.end(),
                        "override is outside allowed datacenters");
                *pool->add_candidates() = node;
            }
            group(check, pg.id()); previous_pg = pg.id();
        }

    }
    std::uint64_t previous_job = 0;
    std::set<std::pair<std::string, std::uint32_t>> pending;
    for (const auto& job : catalog.recoveries()) {
        require(job.id() > previous_job && job.id() <= catalog.recovery_sequence() && job.new_pg().epoch() == job.id() &&
                job.old_pg().epoch() < job.id() && wire::RecoveryPhase_IsValid(job.phase()) &&
                job.old_pg().id() == job.pg_id() && job.new_pg().id() == job.pg_id() && !job.failed_servers().empty(), "invalid recovery record");
        require(names.contains(job.space_name()), "recovery references unknown Space");
        const auto state = std::find_if(catalog.spaces().begin(), catalog.spaces().end(), [&](const auto& item) { return item.space().name() == job.space_name(); });
        require(owns_pg(*state, job.pg_id()) && job.old_pg().active() && job.new_pg().active() &&
                !job.old_pg().recovering() && !job.new_pg().recovering() && job.error().size() <= 4096, "invalid recovery PG state");
        std::set<std::string> old_members, new_members, failed_members;
        for (const auto* pg : {&job.old_pg(), &job.new_pg()}) {
            require(pg->replicas_size() == static_cast<int>(state->space().spec().replicas()), "invalid recovery replica count");
            wire::SpaceState check; *check.mutable_space() = state->space();
            auto* range = check.add_ranges(); range->set_begin(job.pg_id()); range->set_end(job.pg_id() + 1); range->set_algorithm_version(1);
            std::set<std::string> endpoints;
            auto& members = pg == &job.old_pg() ? old_members : new_members;
            for (const auto& node : pg->replicas()) {
                require(!node.server_id().empty() && !node.endpoint().empty() && members.insert(node.server_id()).second &&
                        endpoints.insert(node.endpoint()).second, "invalid recovery member");
                const auto& allowed = state->space().spec().placement_policy().datacenters();
                require(allowed.empty() || std::find(allowed.begin(), allowed.end(), node.datacenter()) != allowed.end(), "recovery outside allowed datacenters");
                *range->add_candidates() = node;
            }
            group(check, job.pg_id());
        }
        for (const auto& failed : job.failed_servers()) require(old_members.contains(failed) && !new_members.contains(failed) &&
            failed_members.insert(failed).second, "invalid failed recovery member");
        require(failed_members.size() < old_members.size(), "recovery has no surviving source");
        for (const auto& node : old_members) require(new_members.contains(node) || failed_members.contains(node), "unrecorded removed replica");
        require(job.cursor().empty() || ((job.phase() == wire::SEALING || job.phase() == wire::COPYING || job.phase() == wire::VERIFYING) &&
                job.cursor().size() == 30 && valid_storage_chunk_id(job.cursor()) && ChunkId::parse(job.cursor()).pg() == job.pg_id()), "invalid recovery cursor");
        if (job.phase() != wire::RECOVERED) require(pending.emplace(job.space_name(), job.pg_id()).second, "overlapping PG recoveries");
        if (job.phase() != wire::RECOVERED) {
            auto blocked = job.old_pg(); blocked.set_recovering(true);
            require(group(*state, job.pg_id()).SerializeAsString() == blocked.SerializeAsString(), "active recovery route differs from old placement");
        }
        previous_job = job.id();
    }
    for (const auto& state : catalog.spaces()) for (const auto& pg : state.overrides()) {
        require(!pg.recovering() || pending.contains({state.space().name(), pg.id()}), "repair route has no recovery job");
    }
    std::set<std::string> failures;
    for (const auto& failure : catalog.failures()) require(!failure.server_id().empty() && failure.since_unix_ms() && failures.insert(failure.server_id()).second,
                                                         "invalid server failure record");
    std::sort(global_ranges.begin(), global_ranges.end());
    std::uint32_t pg_end = 0;
    for (const auto& range : global_ranges) {
        require(range.first >= pg_end, "global PG belongs to more than one Space");
        pg_end = range.second;
    }
    std::sort(blob_ranges.begin(), blob_ranges.end());
    std::uint64_t end = 0;
    for (const auto& range : blob_ranges) {
        require(range.first > end, "Blob sequence reservations overlap between Spaces");
        end = range.second;
    }
    require(end == catalog.reserved_blob_sequence(), "Blob sequence high watermark is inconsistent");
}

void validate_topology(const wire::SpaceCatalog& catalog, const wire::Topology& topology) {
    std::unordered_map<std::string, const wire::FServer*> servers;
    for (const auto& server : topology.servers()) servers.emplace(server.id(), &server);
    auto check_target = [&](const wire::ReplicaTarget& node) {
        const auto found = servers.find(node.server_id());
        if (found == servers.end() || found->second->endpoint() != node.endpoint() || found->second->datacenter() != node.datacenter()) {
            throw Error(wire::CONFLICT, "FServer is referenced by PG recovery: " + node.server_id());
        }
    };
    for (const auto& job : catalog.recoveries()) {
        if (job.phase() != wire::RECOVERED) for (const auto& node : job.new_pg().replicas()) check_target(node);
    }
    for (const auto& state : catalog.spaces()) for (const auto& pg : state.overrides()) for (const auto& node : pg.replicas()) check_target(node);
    for (const auto& state : catalog.spaces()) {
        for (const auto& range : state.ranges()) {
            for (const auto& node : range.candidates()) {
                const auto it = servers.find(node.server_id());
                if (it == servers.end() || it->second->endpoint() != node.endpoint() ||
                    it->second->datacenter() != node.datacenter()) {
                    throw Error(wire::CONFLICT, "FServer is referenced by space placement; removal/address/location change requires migration: " + node.server_id());
                }
            }
        }
    }
}

std::string chunk_id(std::uint32_t space, std::uint64_t sequence, std::uint64_t index) {
    require(space <= 255, "space ID exceeds 8 bits");
    return ChunkId{sequence, static_cast<std::uint8_t>(space), index}.str();
}

DecodedId decode_id(std::string_view id) {
    try {
        const auto value = ChunkId::parse(id);
        require(value.seq_id != 0, "Blob sequence must be nonzero");
        return {value.space_id, value.seq_id, value.index, value.pg()};
    } catch (const std::invalid_argument& error) {
        throw Error(wire::INVALID_ARGUMENT, error.what());
    }
}

bool owns_pg(const wire::SpaceState& state, std::uint32_t pg) {
    const auto range = std::lower_bound(state.ranges().begin(), state.ranges().end(), pg,
        [](const wire::PGRange& value, std::uint32_t id) { return value.end() <= id; });
    return range != state.ranges().end() && range->begin() <= pg;
}

std::uint64_t initial_cursor(const wire::SpaceState& state, std::uint64_t sequence) {
    const auto count = state.space().bound_pgs();
    if (!count) throw Error(wire::CONFLICT, "space has no bound PGs");
    // Choose an ordinal among owned PGs, rather than rounding a random global
    // ID up to a bound interval: rounding would bias sparse ranges' first PGs.
    auto ordinal = hash_chunk_index(sequence) % count;
    for (const auto& range : state.ranges()) {
        const auto length = range.end() - range.begin();
        if (ordinal < length) return range.begin() + ordinal;
        ordinal -= length;
    }
    throw Error(wire::STORAGE_ERROR, "bound PG count is inconsistent");
}

std::optional<std::uint64_t> next_cursor(const wire::SpaceState& state, std::uint64_t cursor) {
    if (state.ranges().empty()) return std::nullopt;
    const auto pg = static_cast<std::uint32_t>(cursor % pg_count);
    const auto range = std::lower_bound(state.ranges().begin(), state.ranges().end(), pg,
        [](const wire::PGRange& value, std::uint32_t id) { return value.end() <= id; });
    const std::uint64_t skip = range == state.ranges().end() ?
        static_cast<std::uint64_t>(pg_count - pg) + state.ranges(0).begin() :
        std::max(pg, range->begin()) - pg;
    if (skip > UINT64_MAX - cursor) return std::nullopt;
    return cursor + skip;
}

bool owns_blob(const wire::SpaceState& state, std::uint64_t sequence) {
    const auto range = std::lower_bound(state.blob_sequences().begin(), state.blob_sequences().end(), sequence,
        [](const wire::BlobSequenceRange& item, std::uint64_t value) { return item.end() < value; });
    return range != state.blob_sequences().end() && range->begin() <= sequence;
}
} // namespace chunkstore::master::placement
