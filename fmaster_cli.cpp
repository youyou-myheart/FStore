#include "fmaster_client.hpp"

#include <google/protobuf/struct.pb.h>
#include <google/protobuf/text_format.h>
#include <google/protobuf/util/json_util.h>

#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <optional>
#include <string_view>
#include <thread>
#include <vector>

namespace {
namespace master = chunkstore::master;
namespace wire = fstore::master::v1;
using JsonObject = google::protobuf::Struct;

constexpr std::string_view usage =
    "usage: fmaster_cli ENDPOINT COMMAND [ARGUMENTS] [OPTIONS]\n"
    "       fmaster_cli --help\n"
    "\n"
    "  list                              Full configuration and status as JSON\n"
    "  status                            Cluster server and disk counts\n"
    "  servers [--online|--offline] [--enabled|--disabled]\n"
    "  server ID                         Configuration and status for one server\n"
    "  disks [SERVER_ID] [--health=unknown|healthy|error|stale]\n"
    "  disk SERVER_ID DISK_ID             Configuration and status for one disk\n"
    "  capacity [SERVER_ID]               Per-disk capacity in bytes\n"
    "  errors [SERVER_ID]                 Server probe errors and unhealthy disks\n"
    "  config                            Export configuration as protobuf text\n"
    "  validate CONFIG_FILE              Validate configuration without applying it\n"
    "  probe [SERVER_ID]                  Schedule an immediate status probe\n"
    "  watch [--interval-ms=N] [--count=N]  Repeated status, default interval 1000 ms\n"
    "  server-set ID ADDRESS:PORT [enabled|disabled]\n"
    "  server-enable ID | server-disable ID | server-remove ID\n"
    "  disk-set SERVER_ID DISK_ID ABS_PATH [enabled|disabled]\n"
    "  disk-enable SERVER_ID DISK_ID | disk-disable SERVER_ID DISK_ID\n"
    "  disk-remove SERVER_ID DISK_ID\n"
    "  apply CONFIG_FILE\n"
    "\n"
    "Views accept --json. list, validate and probe always return JSON.\n"
    "Mutation commands accept a trailing --expected-revision=N.\n";

enum class Command {
    List, Status, Servers, Server, Disks, Disk, Capacity, Errors, Config, Validate,
    Probe, Watch, ServerSet, ServerEnable, ServerDisable, ServerRemove,
    DiskSet, DiskEnable, DiskDisable, DiskRemove, Apply
};
enum Option : unsigned { Json = 1, Revision = 2, Online = 4, Enabled = 8, Health = 16, Watch = 32 };
struct Specification {
    std::string_view name;
    Command command;
    std::size_t minimum;
    std::size_t maximum;
    unsigned options;
};
constexpr std::array specifications{
    Specification{"list", Command::List, 0, 0, Json},
    Specification{"status", Command::Status, 0, 0, Json},
    Specification{"servers", Command::Servers, 0, 0, Json | Online | Enabled},
    Specification{"server", Command::Server, 1, 1, Json},
    Specification{"disks", Command::Disks, 0, 1, Json | Health},
    Specification{"disk", Command::Disk, 2, 2, Json},
    Specification{"capacity", Command::Capacity, 0, 1, Json},
    Specification{"errors", Command::Errors, 0, 1, Json},
    Specification{"config", Command::Config, 0, 0, 0},
    Specification{"validate", Command::Validate, 1, 1, Json},
    Specification{"probe", Command::Probe, 0, 1, Json},
    Specification{"watch", Command::Watch, 0, 0, Json | Watch},
    Specification{"server-set", Command::ServerSet, 2, 3, Revision},
    Specification{"server-enable", Command::ServerEnable, 1, 1, Revision},
    Specification{"server-disable", Command::ServerDisable, 1, 1, Revision},
    Specification{"server-remove", Command::ServerRemove, 1, 1, Revision},
    Specification{"disk-set", Command::DiskSet, 3, 4, Revision},
    Specification{"disk-enable", Command::DiskEnable, 2, 2, Revision},
    Specification{"disk-disable", Command::DiskDisable, 2, 2, Revision},
    Specification{"disk-remove", Command::DiskRemove, 2, 2, Revision},
    Specification{"apply", Command::Apply, 1, 1, Revision}
};

std::uint64_t number(std::string_view value, std::string_view option) {
    std::uint64_t result;
    const auto [end, error] = std::from_chars(value.data(), value.data() + value.size(), result);
    if (error != std::errc{} || end != value.data() + value.size()) {
        throw std::invalid_argument(std::string(option) + " requires an unsigned integer");
    }
    return result;
}

bool disabled(std::string_view value) {
    if (value == "enabled") return false;
    if (value == "disabled") return true;
    throw std::invalid_argument("state must be enabled or disabled");
}

wire::DiskHealth health_value(std::string_view value) {
    if (value == "unknown") return wire::DISK_UNKNOWN;
    if (value == "healthy") return wire::DISK_HEALTHY;
    if (value == "error") return wire::DISK_ERROR;
    if (value == "stale") return wire::DISK_STALE;
    throw std::invalid_argument("health must be unknown, healthy, error or stale");
}

std::string health_name(wire::DiskHealth health) {
    switch (health) {
    case wire::DISK_HEALTHY: return "healthy";
    case wire::DISK_ERROR: return "error";
    case wire::DISK_STALE: return "stale";
    default: return "unknown";
    }
}

struct Arguments {
    explicit Arguments(const Specification& value) : specification(value) {}
    const Specification& specification;
    std::vector<std::string> parameters;
    bool json = false;
    std::optional<std::uint64_t> revision;
    std::optional<bool> online;
    std::optional<bool> enabled;
    std::optional<wire::DiskHealth> health;
    std::optional<std::uint64_t> count;
    std::chrono::milliseconds interval{1000};
};

template<class Value>
void once(std::optional<Value>& destination, Value value, std::string_view option) {
    if (destination) throw std::invalid_argument("repeated or conflicting option " + std::string(option));
    destination = value;
}

Arguments parse(int argc, char** argv) {
    if (argc < 3) throw std::invalid_argument("endpoint and command are required; use --help");
    const auto specification = std::find_if(specifications.begin(), specifications.end(), [&](const auto& item) {
        return item.name == argv[2];
    });
    if (specification == specifications.end()) throw std::invalid_argument("unknown command; use --help");
    Arguments args{*specification};
    bool interval_set = false;
    for (int i = 3; i < argc; ++i) {
        const std::string_view value = argv[i];
        if (!value.starts_with("--")) {
            args.parameters.emplace_back(value);
            continue;
        }
        unsigned category = 0;
        if (value == "--json") {
            if (args.json) throw std::invalid_argument("repeated --json option");
            args.json = true;
            category = Json;
        } else if (value == "--online" || value == "--offline") {
            once(args.online, value == "--online", value);
            category = Online;
        } else if (value == "--enabled" || value == "--disabled") {
            once(args.enabled, value == "--enabled", value);
            category = Enabled;
        } else if (value.starts_with("--health=")) {
            once(args.health, health_value(value.substr(9)), value);
            category = Health;
        } else if (value.starts_with("--expected-revision=")) {
            if (i + 1 != argc) throw std::invalid_argument("--expected-revision must be the final argument");
            once(args.revision, number(value.substr(20), value), value);
            category = Revision;
        } else if (value.starts_with("--count=")) {
            const auto count = number(value.substr(8), value);
            if (!count) throw std::invalid_argument("--count must be positive");
            once(args.count, count, value);
            category = Watch;
        } else if (value.starts_with("--interval-ms=")) {
            if (interval_set) throw std::invalid_argument("repeated --interval-ms option");
            const auto interval = number(value.substr(14), value);
            if (!interval || interval > std::numeric_limits<int>::max()) {
                throw std::invalid_argument("--interval-ms must be in 1..INT_MAX");
            }
            args.interval = std::chrono::milliseconds(interval);
            interval_set = true;
            category = Watch;
        } else {
            throw std::invalid_argument("unknown option " + std::string(value));
        }
        if (!(args.specification.options & category)) {
            throw std::invalid_argument("option is not supported for this command: " + std::string(value));
        }
    }
    if (args.parameters.size() < specification->minimum || args.parameters.size() > specification->maximum) {
        throw std::invalid_argument("invalid arguments for " + std::string(specification->name) + "; use --help");
    }
    return args;
}

template<class Request>
void set_revision(Request& request, const Arguments& args) {
    if (args.revision) request.set_expected_revision(*args.revision);
}

wire::Topology load_config(const std::string& path) {
    std::ifstream input(path, std::ios::binary);
    if (!input) throw std::runtime_error("could not open configuration file " + path);
    constexpr std::size_t limit = 16 * 1024 * 1024;
    std::string contents(limit + 1, '\0');
    input.read(contents.data(), static_cast<std::streamsize>(contents.size()));
    if (input.bad()) throw std::runtime_error("could not read configuration file " + path);
    contents.resize(static_cast<std::size_t>(input.gcount()));
    if (contents.size() > limit) throw std::length_error("configuration file exceeds 16 MiB");
    wire::Topology topology;
    if (!google::protobuf::TextFormat::ParseFromString(contents, &topology)) {
        throw std::invalid_argument("configuration must be a protobuf text-format Topology");
    }
    return topology;
}

void json(const google::protobuf::Message& message) {
    google::protobuf::util::JsonPrintOptions options;
    options.always_print_primitive_fields = true;
    options.preserve_proto_field_names = true;
    std::string output;
    if (!google::protobuf::util::MessageToJsonString(message, &output, options).ok()) {
        throw std::runtime_error("could not serialize FMaster response to JSON");
    }
    std::cout << output << '\n';
}

void text_field(JsonObject& object, const std::string& key, const std::string& value) {
    (*object.mutable_fields())[key].set_string_value(value);
}

const wire::ServerStatus& server_status(const wire::ListResponse& response, const std::string& id) {
    const auto found = std::find_if(response.servers().begin(), response.servers().end(), [&](const auto& item) {
        return item.server_id() == id;
    });
    if (found == response.servers().end()) throw std::runtime_error("missing server status in FMaster response");
    return *found;
}

const wire::DiskReport& disk_report(const wire::ServerStatus& status, const std::string& id) {
    const auto found = std::find_if(status.disks().begin(), status.disks().end(), [&](const auto& item) {
        return item.id() == id;
    });
    if (found == status.disks().end()) throw std::runtime_error("missing disk status in FMaster response");
    return *found;
}

wire::ListResponse filtered(const wire::ListResponse& source, const Arguments& args) {
    wire::ListResponse result;
    *result.mutable_status() = source.status();
    result.mutable_topology()->set_revision(source.topology().revision());
    const auto command = args.specification.command;
    bool found_disk = false;
    for (const auto& server : source.topology().servers()) {
        const auto& status = server_status(source, server.id());
        if (args.online && status.online() != *args.online) continue;
        if (args.enabled && server.disabled() == *args.enabled) continue;
        auto selected_server = server;
        auto selected_status = status;
        selected_server.clear_disks();
        selected_status.clear_disks();
        for (const auto& disk : server.disks()) {
            const auto& report = disk_report(status, disk.id());
            if (command == Command::Disk && disk.id() != args.parameters[1]) continue;
            if (args.health && report.health() != *args.health) continue;
            if (command == Command::Errors && report.health() == wire::DISK_HEALTHY) continue;
            *selected_server.add_disks() = disk;
            *selected_status.add_disks() = report;
            found_disk = true;
        }
        if (command == Command::Errors && status.online() && status.error().empty() &&
            selected_server.disks().empty()) continue;
        if ((command == Command::Disks || command == Command::Disk) && selected_server.disks().empty()) continue;
        *result.mutable_topology()->add_servers() = std::move(selected_server);
        *result.add_servers() = std::move(selected_status);
    }
    if (command == Command::Disk && !found_disk) throw master::RemoteError(wire::NOT_FOUND, "disk not found");
    return result;
}

std::string printable(std::string_view value) {
    std::string result;
    for (const unsigned char character : value) {
        if (character == '\n') result += "\\n";
        else if (character == '\r') result += "\\r";
        else if (character == '\t') result += "\\t";
        else if (character < 32 || character == 127) result += '?';
        else result += static_cast<char>(character);
    }
    return result;
}

void table(std::vector<std::string> headings, std::vector<std::vector<std::string>> rows) {
    std::vector<std::size_t> widths;
    for (const auto& heading : headings) widths.push_back(heading.size());
    for (auto& row : rows) {
        for (std::size_t i = 0; i < row.size(); ++i) {
            row[i] = printable(row[i]);
            widths[i] = std::max(widths[i], row[i].size());
        }
    }
    auto print_row = [&](const std::vector<std::string>& row) {
        for (std::size_t i = 0; i < row.size(); ++i) {
            if (i + 1 == row.size()) std::cout << row[i];
            else std::cout << std::left << std::setw(static_cast<int>(widths[i] + 2)) << row[i];
        }
        std::cout << '\n';
    };
    print_row(headings);
    for (const auto& row : rows) print_row(row);
}

void show_servers(const wire::ListResponse& response) {
    std::vector<std::vector<std::string>> rows;
    for (const auto& server : response.topology().servers()) {
        const auto& status = server_status(response, server.id());
        rows.push_back({server.id(), server.endpoint(), server.disabled() ? "disabled" : "enabled",
                        status.online() ? "online" : "offline", std::to_string(server.disks_size()),
                        std::to_string(status.last_heartbeat_unix_ms()), std::to_string(status.last_probe_unix_ms()),
                        status.error()});
    }
    table({"ID", "ENDPOINT", "STATE", "STATUS", "DISKS", "LAST_HEARTBEAT_MS", "LAST_PROBE_MS", "ERROR"}, std::move(rows));
}

bool known_capacity(const wire::DiskReport& report) {
    return report.health() == wire::DISK_HEALTHY ||
           (report.health() == wire::DISK_STALE && report.observed_unix_ms() && report.error().empty());
}

void show_disks(const wire::ListResponse& response) {
    std::vector<std::vector<std::string>> rows;
    for (const auto& server : response.topology().servers()) {
        const auto& status = server_status(response, server.id());
        for (const auto& disk : server.disks()) {
            const auto& report = disk_report(status, disk.id());
            const bool known = known_capacity(report);
            rows.push_back({server.id(), disk.id(), disk.path(), disk.disabled() ? "disabled" : "enabled",
                            health_name(report.health()), known ? std::to_string(report.total_bytes()) : "-",
                            known ? std::to_string(report.free_bytes()) : "-",
                            known ? std::to_string(report.available_bytes()) : "-",
                            std::to_string(report.observed_unix_ms()), report.error()});
        }
    }
    table({"SERVER", "DISK", "PATH", "STATE", "HEALTH", "TOTAL_BYTES", "FREE_BYTES", "AVAILABLE_BYTES", "OBSERVED_MS", "ERROR"},
          std::move(rows));
}

void show_status(const wire::ListResponse& response, bool as_json) {
    std::uint64_t online = 0, enabled = 0, disks = 0, disks_enabled = 0;
    std::array<std::uint64_t, 4> health{};
    for (const auto& server : response.topology().servers()) {
        const auto& status = server_status(response, server.id());
        online += status.online();
        enabled += !server.disabled();
        for (const auto& disk : server.disks()) {
            ++disks;
            disks_enabled += !disk.disabled();
            const auto value = disk_report(status, disk.id()).health();
            const auto index = wire::DiskHealth_IsValid(value) ? static_cast<std::size_t>(value) : 0;
            ++health[index];
        }
    }
    const auto servers = static_cast<std::uint64_t>(response.topology().servers_size());
    if (as_json) {
        JsonObject output;
        text_field(output, "revision", std::to_string(response.topology().revision()));
        auto* server_counts = (*output.mutable_fields())["servers"].mutable_struct_value();
        auto* disk_counts = (*output.mutable_fields())["disks"].mutable_struct_value();
        for (const auto& [key, value] : std::initializer_list<std::pair<std::string, std::uint64_t>>{
                {"total", servers}, {"online", online}, {"offline", servers - online},
                {"enabled", enabled}, {"disabled", servers - enabled}}) {
            (*server_counts->mutable_fields())[key].set_number_value(static_cast<double>(value));
        }
        for (const auto& [key, value] : std::initializer_list<std::pair<std::string, std::uint64_t>>{
                {"total", disks}, {"enabled", disks_enabled}, {"disabled", disks - disks_enabled},
                {"unknown", health[0]}, {"healthy", health[1]}, {"error", health[2]}, {"stale", health[3]}}) {
            (*disk_counts->mutable_fields())[key].set_number_value(static_cast<double>(value));
        }
        json(output);
        return;
    }
    table({"REVISION", "SERVERS", "ONLINE", "OFFLINE", "ENABLED", "DISABLED", "DISKS", "DISK_ENABLED", "DISK_DISABLED", "UNKNOWN", "HEALTHY", "ERROR", "STALE"},
          {{std::to_string(response.topology().revision()), std::to_string(servers), std::to_string(online),
            std::to_string(servers - online), std::to_string(enabled), std::to_string(servers - enabled),
            std::to_string(disks), std::to_string(disks_enabled), std::to_string(disks - disks_enabled),
            std::to_string(health[0]), std::to_string(health[1]), std::to_string(health[2]), std::to_string(health[3])}});
}

void show_capacity(const wire::ListResponse& response, bool as_json) {
    JsonObject output;
    text_field(output, "revision", std::to_string(response.topology().revision()));
    auto* list = (*output.mutable_fields())["disks"].mutable_list_value();
    std::vector<std::vector<std::string>> rows;
    for (const auto& server : response.topology().servers()) {
        const auto& status = server_status(response, server.id());
        for (const auto& disk : server.disks()) {
            const auto& report = disk_report(status, disk.id());
            const bool known = known_capacity(report);
            auto* item = list->add_values()->mutable_struct_value();
            text_field(*item, "server_id", server.id());
            text_field(*item, "id", disk.id());
            text_field(*item, "path", disk.path());
            text_field(*item, "health", health_name(report.health()));
            text_field(*item, "observed_unix_ms", std::to_string(report.observed_unix_ms()));
            text_field(*item, "error", report.error());
            const auto used = report.total_bytes() - std::min(report.total_bytes(), report.free_bytes());
            for (const auto& [key, value] : std::initializer_list<std::pair<std::string, std::uint64_t>>{
                    {"total_bytes", report.total_bytes()}, {"used_bytes", used},
                    {"free_bytes", report.free_bytes()}, {"available_bytes", report.available_bytes()}}) {
                auto& field = (*item->mutable_fields())[key];
                if (known) field.set_string_value(std::to_string(value));
                else field.set_null_value(google::protobuf::NULL_VALUE);
            }
            rows.push_back({server.id(), disk.id(), disk.path(), health_name(report.health()),
                            known ? std::to_string(report.total_bytes()) : "-", known ? std::to_string(used) : "-",
                            known ? std::to_string(report.free_bytes()) : "-",
                            known ? std::to_string(report.available_bytes()) : "-",
                            std::to_string(report.observed_unix_ms()), report.error()});
        }
    }
    if (as_json) json(output);
    else table({"SERVER", "DISK", "PATH", "HEALTH", "TOTAL_BYTES", "USED_BYTES", "FREE_BYTES", "AVAILABLE_BYTES", "OBSERVED_MS", "ERROR"},
               std::move(rows));
}

std::uint64_t mutate(const master::Client& client, const Arguments& args) {
    const auto& parameters = args.parameters;
    switch (args.specification.command) {
    case Command::ServerSet: {
        wire::SetServerRequest request;
        request.set_id(parameters[0]);
        request.set_endpoint(parameters[1]);
        request.set_disabled(parameters.size() == 3 && disabled(parameters[2]));
        set_revision(request, args);
        return client.set_server(request);
    }
    case Command::ServerEnable:
    case Command::ServerDisable: {
        wire::SetServerStateRequest request;
        request.set_id(parameters[0]);
        request.set_disabled(args.specification.command == Command::ServerDisable);
        set_revision(request, args);
        return client.set_server_state(request);
    }
    case Command::ServerRemove: {
        wire::RemoveServerRequest request;
        request.set_id(parameters[0]);
        set_revision(request, args);
        return client.remove_server(request);
    }
    case Command::DiskSet: {
        wire::SetDiskRequest request;
        request.set_server_id(parameters[0]);
        auto& disk = *request.mutable_disk();
        disk.set_id(parameters[1]);
        disk.set_path(parameters[2]);
        disk.set_disabled(parameters.size() == 4 && disabled(parameters[3]));
        set_revision(request, args);
        return client.set_disk(request);
    }
    case Command::DiskEnable:
    case Command::DiskDisable: {
        wire::SetDiskStateRequest request;
        request.set_server_id(parameters[0]);
        request.set_disk_id(parameters[1]);
        request.set_disabled(args.specification.command == Command::DiskDisable);
        set_revision(request, args);
        return client.set_disk_state(request);
    }
    case Command::DiskRemove: {
        wire::RemoveDiskRequest request;
        request.set_server_id(parameters[0]);
        request.set_disk_id(parameters[1]);
        set_revision(request, args);
        return client.remove_disk(request);
    }
    case Command::Apply: {
        wire::ApplyConfigRequest request;
        *request.mutable_topology() = load_config(parameters[0]);
        set_revision(request, args);
        return client.apply_config(request);
    }
    default: throw std::logic_error("unexpected mutation command");
    }
}

void execute(const master::Client& client, const Arguments& args) {
    const auto command = args.specification.command;
    if (args.specification.options & Revision) {
        std::cout << mutate(client, args) << '\n';
        return;
    }
    if (command == Command::Validate) {
        wire::ValidateConfigRequest request;
        *request.mutable_topology() = load_config(args.parameters[0]);
        json(client.validate_config(request));
        return;
    }
    if (command == Command::Probe) {
        json(client.probe_now(args.parameters.empty() ? "" : args.parameters[0]));
        return;
    }
    if (command == Command::Watch) {
        for (std::uint64_t sample = 0; ; ++sample) {
            show_status(client.list(), args.json);
            std::cout.flush();
            if (!std::cout) throw std::runtime_error("could not write command output");
            if (args.count && sample + 1 >= *args.count) return;
            std::this_thread::sleep_for(args.interval);
        }
    }
    const auto source = client.list(args.parameters.empty() ? "" : args.parameters[0]);
    if (command == Command::List) {
        json(source);
    } else if (command == Command::Config) {
        std::string output;
        if (!google::protobuf::TextFormat::PrintToString(source.topology(), &output)) {
            throw std::runtime_error("could not serialize configuration");
        }
        std::cout << output;
    } else if (command == Command::Status) {
        show_status(source, args.json);
    } else if (command == Command::Capacity) {
        show_capacity(source, args.json);
    } else {
        const auto view = filtered(source, args);
        if (args.json) {
            json(view);
        } else if (command == Command::Servers) {
            show_servers(view);
        } else if (command == Command::Server || command == Command::Errors) {
            show_servers(view);
            std::cout << '\n';
            show_disks(view);
        } else {
            show_disks(view);
        }
    }
}
}

int main(int argc, char** argv) {
    if ((argc == 2 && (std::string_view(argv[1]) == "--help" || std::string_view(argv[1]) == "help")) ||
        (argc == 3 && (std::string_view(argv[2]) == "--help" || std::string_view(argv[2]) == "help"))) {
        std::cout << usage;
        return EXIT_SUCCESS;
    }
    try {
        const auto args = parse(argc, argv);
        master::Client client(argv[1]);
        execute(client, args);
        std::cout.flush();
        if (!std::cout) throw std::runtime_error("could not write command output");
    } catch (const master::RemoteError& error) {
        std::cerr << "FMaster error (" << wire::Code_Name(error.code()) << "): " << error.what() << '\n';
        return EXIT_FAILURE;
    } catch (const master::RpcError& error) {
        std::cerr << "RPC error (" << error.code() << "): " << error.what() << '\n';
        return EXIT_FAILURE;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}
