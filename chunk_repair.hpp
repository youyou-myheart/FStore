#pragma once

#include "chunk.pb.h"
#include "chunk_store.hpp"

#include <butil/iobuf.h>
#include <butil/files/scoped_file.h>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>

namespace chunkstore {

std::uint32_t repair_block_size(const TrafficLimiter& limiter);
TrafficLane copy_lane(rpc::v1::CopyPriority priority);

// Independent receiver slots keep other priorities available during a transfer.
class RepairReceiver {
public:
    explicit RepairReceiver(ChunkStore& store);
    void discard(std::string_view id);
    std::uint64_t receive(const rpc::v1::RepairBlock& block, butil::IOBuf data);
private:
    struct Transfer {
        explicit Transfer(const rpc::v1::RepairBlock& block)
            : id(block.chunk_id()), token(block.transfer_id()),
              info{block.total_length(), block.capacity(), State::Frozen} {}

        std::string id;
        std::string token;
        ReplicaInfo info;
        butil::ScopedFD file;
        bool busy = false; // pins this slot while its worker performs I/O
        std::uint64_t next = 0;
        std::chrono::steady_clock::time_point deadline;
    };
    ChunkStore& store_;
    std::mutex mutex_;
    std::condition_variable_any changed_;
    std::array<std::optional<Transfer>, 3> transfers_;
    std::jthread cleanup_;
};

void push_replica(ChunkStore& store, const rpc::v1::RepairRequest& request,
                  rpc::v1::RepairResponse& response);
}
