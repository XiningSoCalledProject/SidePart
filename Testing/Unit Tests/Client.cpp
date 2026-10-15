//
// Client.cpp — connectivity smoke test for the storage listener  (v2, 9/22/26)
//
// Spawns N Server processes and opens one connection to each, in parallel, to
// verify that the listeners come up and speak the wire protocol.  It is NOT an
// ORAM test: the real ORAM traffic goes through NetIOConnector, driven by the
// children that MultiRingORAM_Servers forks.
//
// v2 fixes three things that made v1 report failures on a healthy system:
//
//   1. WRONG BINARY PATH.  v1 looked in ./Server, ./build/Server, ../Server.
//      CMake puts executables in build/bin, so it never found the server and
//      exited before testing anything.  The search list now covers build/bin
//      and, for Servers_MultiRingORAM, the listener the tests actually use.
//
//   2. PROTOCOL MISMATCH.  v1 sent the plain text "Hello from AutoClient" and
//      then waited for a reply.  The server reads the FIRST BYTE as an opcode:
//      'H' (72) is not a valid opcode, so the server logged "Unknown opcode 72"
//      and closed without replying, and the client sat in recv() until its 5 s
//      timeout and reported "Failed to receive response" for every server.
//      v2 sends opcode 9 (SET_TUPLE_LENGTH) — a harmless, idempotent request
//      that touches no slot data — and then opcode -1 (CLOSE), which is exactly
//      what a well-behaved client does.
//
//   3. THE READINESS PROBE STOLE THE CONNECTION.  v1 called waitForServer(),
//      which connected and immediately closed.  The listener's accept() is
//      satisfied by that probe, so the probe became "connection #1" and the
//      real connection had to wait for the server to loop around and accept
//      again.  v2 drops the probe and simply retries the real connect.
//
// Usage: ./Client [num_servers] [starting_port] [server_exec]
//
#include <arpa/inet.h>
#include <netinet/in.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include <chrono>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

static constexpr int DEFAULT_NUM_SERVERS   = 3;
static constexpr int DEFAULT_STARTING_PORT = 8881;
static constexpr int NAME_LENGTH           = 32;   // must match the server

struct Target {
    std::string ip;
    int   port = 0;
    int   id   = 0;
    pid_t pid  = -1;
};

struct Result {
    int         id = 0;
    std::string address;
    bool        success = false;
    double      elapsed_ms = 0;
    std::string error;
};

static std::vector<pid_t> g_server_pids;
static std::mutex         g_cout_mu;

static void safe_print(const std::string& s) {
    std::lock_guard<std::mutex> lk(g_cout_mu);
    std::cout << s << std::flush;
}

static void cleanup_servers(int signum) {
    std::cout << "\nShutting down servers...\n";
    for (pid_t pid : g_server_pids) kill(pid, SIGTERM);
    _exit(signum);
}

// ── Find the listener binary ────────────────────────────────────────────────
// CMake writes executables to build/bin.  Servers_MultiRingORAM is the listener
// the tests use; Server is the older single-file one.  Both speak this protocol.
static std::string findServerExec(const std::string& override_path) {
    std::vector<std::string> candidates;
    if (!override_path.empty()) candidates.push_back(override_path);
    for (const char* dir : {"./build/bin/", "./bin/", "./", "../build/bin/", "../"})
        for (const char* name : {"Servers_MultiRingORAM", "Server"})
            candidates.push_back(std::string(dir) + name);
    for (const auto& c : candidates)
        if (access(c.c_str(), X_OK) == 0) return c;
    return {};
}

static pid_t startServer(int port, const std::string& exec) {
    pid_t pid = fork();
    if (pid < 0) return -1;
    if (pid == 0) {
        const std::string log = "server_" + std::to_string(port) + ".log";
        if (!freopen(log.c_str(), "w", stdout)) _exit(1);
        if (!freopen(log.c_str(), "w", stderr)) _exit(1);
        const std::string port_str = std::to_string(port);
        execl(exec.c_str(), exec.c_str(), port_str.c_str(), (char*)nullptr);
        _exit(1);                       // execl only returns on failure
    }
    return pid;
}

static bool sendAll(int fd, const void* buf, size_t n) {
    const char* p = static_cast<const char*>(buf);
    while (n) {
        const ssize_t w = send(fd, p, n, 0);
        if (w <= 0) return false;
        p += w; n -= static_cast<size_t>(w);
    }
    return true;
}

// One connection: connect (with retry), speak the protocol, close cleanly.
static Result probeServer(const Target& t) {
    Result r;
    r.id = t.id;
    r.address = t.ip + ":" + std::to_string(t.port);
    const auto t0 = std::chrono::high_resolution_clock::now();

    int fd = -1;
    for (int attempt = 0; attempt < 100; ++attempt) {      // ~10 s total
        fd = socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) { r.error = "socket() failed"; return r; }
        timeval tv{5, 0};
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port   = htons(static_cast<uint16_t>(t.port));
        if (inet_pton(AF_INET, t.ip.c_str(), &addr.sin_addr) <= 0) {
            r.error = "bad address"; close(fd); return r;
        }
        if (connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0) break;
        close(fd);
        fd = -1;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    if (fd < 0) {
        r.error = "connect failed (listener not up?)";
        r.elapsed_ms = std::chrono::duration<double, std::milli>(
                std::chrono::high_resolution_clock::now() - t0).count();
        return r;
    }
    safe_print("  [Server " + std::to_string(t.id) + "] connected to " + r.address + "\n");

    // SET_TUPLE_LENGTH (opcode 9): name[32] + uint32 length, network order.
    // Touches no slot data, so it is safe to send to a live listener.
    const int8_t OP_SET_TUPLE_LENGTH = 9, OP_CLOSE = -1;
    char name[NAME_LENGTH];
    std::memset(name, 0, sizeof(name));
    std::snprintf(name, sizeof(name), "ping_%d", t.id);
    const uint32_t len_net = htonl(64);

    if (!sendAll(fd, &OP_SET_TUPLE_LENGTH, 1) ||
        !sendAll(fd, name, NAME_LENGTH) ||
        !sendAll(fd, &len_net, sizeof(len_net))) {
        r.error = "send failed (server closed early?)";
        close(fd);
        return r;
    }

    // CLOSE (opcode -1): the server logs the session and returns; no reply is
    // sent for either opcode, so we must NOT wait for one.
    if (!sendAll(fd, &OP_CLOSE, 1)) {
        r.error = "send CLOSE failed";
        close(fd);
        return r;
    }
    safe_print("  [Server " + std::to_string(t.id) + "] protocol OK, closed\n");

    close(fd);
    r.success = true;
    r.elapsed_ms = std::chrono::duration<double, std::milli>(
            std::chrono::high_resolution_clock::now() - t0).count();
    return r;
}

int main(int argc, char* argv[]) {
    const int num_servers   = argc > 1 ? std::atoi(argv[1]) : DEFAULT_NUM_SERVERS;
    const int starting_port = argc > 2 ? std::atoi(argv[2]) : DEFAULT_STARTING_PORT;
    const std::string exec_override = argc > 3 ? argv[3] : "";

    if (num_servers < 1 || num_servers > 100) {
        std::cerr << "Invalid number of servers (1-100)\n";
        return 1;
    }

    signal(SIGINT,  cleanup_servers);
    signal(SIGTERM, cleanup_servers);

    const std::string server_exec = findServerExec(exec_override);
    if (server_exec.empty()) {
        std::cerr << "Server executable not found.  Looked for Servers_MultiRingORAM "
                     "and Server under ./build/bin, ./bin, ., ../build/bin, ..\n"
                     "Build it first:  cmake --build build --target Servers_MultiRingORAM\n";
        return 1;
    }

    std::cout << "Listener : " << server_exec << "\n"
              << "Servers  : " << num_servers << " on ports " << starting_port << ".."
              << starting_port + num_servers - 1 << "\n\n";

    std::vector<Target> targets;
    for (int i = 0; i < num_servers; ++i) {
        Target t;
        t.ip = "127.0.0.1";
        t.port = starting_port + i;
        t.id = i + 1;
        t.pid = startServer(t.port, server_exec);
        if (t.pid < 0) {
            std::cerr << "fork failed for port " << t.port << "\n";
            cleanup_servers(1);
        }
        g_server_pids.push_back(t.pid);
        targets.push_back(t);
        std::cout << "  started listener on port " << t.port << " (pid " << t.pid << ")\n";
    }

    std::cout << "\nConnecting in parallel (connect retries for up to 10 s each)...\n";
    std::vector<Result> results(targets.size());
    std::vector<std::thread> threads;
    const auto t0 = std::chrono::high_resolution_clock::now();
    for (size_t i = 0; i < targets.size(); ++i)
        threads.emplace_back([&, i]() { results[i] = probeServer(targets[i]); });
    for (auto& th : threads) th.join();
    const double overall_ms = std::chrono::duration<double, std::milli>(
            std::chrono::high_resolution_clock::now() - t0).count();

    int ok = 0;
    std::cout << "\n" << std::string(58, '=') << "\n";
    for (const auto& r : results) {
        std::cout << "Server " << r.id << " (" << r.address << "): "
                  << (r.success ? "OK" : "FAILED");
        if (!r.success) std::cout << " — " << r.error;
        std::cout << "  (" << std::fixed << std::setprecision(1) << r.elapsed_ms << " ms)\n";
        ok += r.success ? 1 : 0;
    }
    std::cout << std::string(58, '=') << "\n"
              << ok << "/" << results.size() << " listeners healthy, "
              << std::fixed << std::setprecision(1) << overall_ms << " ms total\n"
              << "Per-server logs: server_<port>.log\n";

    for (const auto& t : targets) {
        kill(t.pid, SIGTERM);
        int status = 0;
        waitpid(t.pid, &status, 0);
    }
    return ok == static_cast<int>(results.size()) ? 0 : 1;
}