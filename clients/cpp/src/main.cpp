#include "gdrive_fuse/Auth.hpp"
#include "gdrive_fuse/FuseOps.hpp"
#include "gdrive_fuse/GClient.hpp"

#include <spdlog/spdlog.h>

#include <cstring>
#include <iostream>
#include <memory>

void print_usage(const char* program_name) {
    std::cout << "Usage: " << program_name << " [options] <mountpoint>\n"
              << "\nOptions:\n"
              << "  --client-id <id>        OAuth2 client ID (required)\n"
              << "  --client-secret <secret> OAuth2 client secret (required)\n"
              << "  --debug                 Enable debug logging\n"
              << "  --version, -V           Print version and exit\n"
              << "  --help                  Show this help message\n"
              << "\nFUSE options can be passed after the mountpoint.\n"
              << "\nExample:\n"
              << "  " << program_name << " --client-id abc123 --client-secret xyz789 /mnt/gdrive\n"
              << std::endl;
}

int main(int argc, char* argv[]) {
    // Parse command line arguments
    std::string client_id;
    std::string client_secret;
    bool debug    = false;
    int fuse_argc = 0;
    // Store pointers to argv elements for FUSE
    // These pointers remain valid for the lifetime of main()
    std::vector<char*> fuse_argv_vec;
    fuse_argv_vec.reserve(argc);

    for (int i = 0; i < argc; ++i) {
        std::string arg = argv[i];

        if (arg == "--help" || arg == "-h") {
            print_usage(argv[0]);
            return 0;
        } else if (arg == "--version" || arg == "-V") {
            std::cout << "gdrive-fuse-cpp " << GDRIVE_FUSE_VERSION << "\n";
            return 0;
        } else if (arg == "--client-id" && i + 1 < argc) {
            client_id = argv[++i];
        } else if (arg == "--client-secret" && i + 1 < argc) {
            client_secret = argv[++i];
        } else if (arg == "--debug") {
            debug = true;
        } else {
            // Pass to FUSE
            fuse_argv_vec.push_back(argv[i]);
            fuse_argc++;
        }
    }

    // Setup logging
    if (debug) {
        spdlog::set_level(spdlog::level::debug);
    } else {
        spdlog::set_level(spdlog::level::info);
    }

    spdlog::info("Google Drive FUSE client starting...");

    // Validate arguments
    if (client_id.empty() || client_secret.empty()) {
        spdlog::error("Client ID and client secret are required");
        print_usage(argv[0]);
        return 1;
    }

    if (fuse_argc < 2) {
        spdlog::error("Mountpoint is required");
        print_usage(argv[0]);
        return 1;
    }

    try {
        // Create Auth object and authenticate
        auto auth = std::make_shared<gdrive_fuse::Auth>(client_id, client_secret);

        spdlog::info("Starting authentication...");
        if (!auth->authenticate()) {
            spdlog::error("Authentication failed");
            return 1;
        }

        spdlog::info("Authentication successful");

        // Create GClient
        auto client = std::make_shared<gdrive_fuse::GClient>(auth);

        // Create FuseOps
        auto fuse_ops = std::make_unique<gdrive_fuse::FuseOps>(client);
        gdrive_fuse::FuseOps::setInstance(fuse_ops.get());

        // Get FUSE operations
        auto ops = gdrive_fuse::FuseOps::getFuseOperations();

        spdlog::info("Mounting filesystem...");

        // Run FUSE main loop
        int ret = fuse_main(fuse_argc, fuse_argv_vec.data(), &ops, nullptr);

        spdlog::info("Filesystem unmounted");

        return ret;
    } catch (const std::exception& e) {
        spdlog::error("Fatal error: {}", e.what());
        return 1;
    }
}
