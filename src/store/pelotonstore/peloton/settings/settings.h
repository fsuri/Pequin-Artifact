//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// configuration.h
//
// Identification: src/include/configuration/configuration.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

//===----------------------------------------------------------------------===//
// FILE LOCATIONS
//===----------------------------------------------------------------------===//

//===----------------------------------------------------------------------===//
// CONNECTIONS
//===----------------------------------------------------------------------===//
// Peloton port
SETTING_int(peloton_port,
           "Peloton port (default: 15721)",
           15721,
           1024, 65535,
           false, false)

// Maximum number of connections
SETTING_int(peloton_max_connections,
           "Maximum number of connections (default: 64)",
           64,
           1, 512,
           true, true)

SETTING_int(peloton_rpc_port,
            "Peloton rpc port (default: 15445)",
            15445,
            1024, 65535,
            false, false)

// TODO(tianyu): Remove when we change to a different rpc framework
// This is here only because capnp cannot exit gracefully and thus causes
// test failure. This is an issue with the capnp implementation and has
// been such way for a while, so it's unlikely it gets fixed.
// See: https://groups.google.com/forum/#!topic/capnproto/bgxCdqGD6oE
SETTING_bool(peloton_rpc_enabled,
             "Enable rpc, this should be turned off when testing",
             false, false, false)

// Socket family
SETTING_string(peloton_socket_family,
              "Socket family (default: AF_INET)",
              "AF_INET",
              false, false)

// Added for SSL only begins

// Enables SSL connection. The default value is false
SETTING_bool(peloton_ssl, "Enable SSL connection (default: true)", false, false, false)

// Peloton private key file
// Currently use hardcoded private key path, may need to change
// to generate file dynamically at runtime
// The same applies to certificate file
SETTING_string(peloton_private_key_file,
              "path to private key file",
              "peloton_insecure_server.key",
              false, false)

// Peloton certificate file
SETTING_string(peloton_certificate_file,
              "path to certificate file",
              "peloton_insecure_server.crt",
              false, false)

// Peloton root certificate file
SETTING_string(peloton_root_cert_file,
               "path to root certificate file",
               "root.crt",
               false, false)

//===----------------------------------------------------------------------===//
// RESOURCE USAGE
//===----------------------------------------------------------------------===//

SETTING_double(peloton_bnlj_buffer_size,
             "The default buffer size to use for blockwise nested loop joins (default: 1 MB)",
             1.0 * 1024.0 * 1024.0,
             1.0 * 1024,
             1.0 * 1024.0 * 1024.0 * 1024,
             true, true)

// Size of the MonoQueue task queue
SETTING_int(peloton_monoqueue_task_queue_size,
            "MonoQueue Task Queue Size (default: 32)",
            32, 
            8, 128,
            false, false)

// Size of the MonoQueue worker pool
SETTING_int(peloton_monoqueue_worker_pool_size,
            "MonoQueue Worker Pool Size (default: 4)",
            0, //4,    //FS DISABLED threadpool: Traffic cop executes directly
            0, 32, //1, 32,
            false, false)

// Number of connection threads used by peloton
SETTING_int(peloton_connection_thread_count,
            "Number of connection threads (default: std::hardware_concurrency())",
            std::thread::hardware_concurrency(),
            1, 64,
            false, false)

SETTING_int(peloton_gc_num_threads,
            "The number of Garbage collection threads to run",
            1,
            1, 128,
            true, true)

SETTING_bool(peloton_parallel_execution,
             "Enable parallel execution of queries (default: true)",
             true,
             true, true)

SETTING_int(peloton_min_parallel_table_scan_size,
            "Minimum number of tuples a table must have before we consider performing parallel scans (default: 10K)",
            10 * 1000,
            1, std::numeric_limits<int32_t>::max(),
            true, true)

//===----------------------------------------------------------------------===//
// WRITE AHEAD LOG
//===----------------------------------------------------------------------===//

//===----------------------------------------------------------------------===//
// ERROR REPORTING AND LOGGING
//===----------------------------------------------------------------------===//

//===----------------------------------------------------------------------===//
// STATISTICS
//===----------------------------------------------------------------------===//

// Enable or disable statistics collection
SETTING_int(peloton_stats_mode,
           "Enable statistics collection (default: 0)",
           static_cast<int>(peloton_peloton::StatsType::INVALID),
           0, 16,
           true, true)

//===----------------------------------------------------------------------===//
// AI
//===----------------------------------------------------------------------===//

// Enable or disable index tuner
SETTING_bool(peloton_index_tuner,
            "Enable index tuner (default: false)",
            false,
            true, true)

// Enable or disable layout tuner
SETTING_bool(peloton_layout_tuner,
            "Enable layout tuner (default: false)",
            false,
            true, true)

//===----------------------------------------------------------------------===//
// BRAIN
//===----------------------------------------------------------------------===//

// Enable or disable brain
SETTING_bool(peloton_brain,
            "Enable brain (default: false)",
            false,
            true, true)

SETTING_string(peloton_peloton_address,
               "ip and port of the peloton rpc service, address:port",
               "127.0.0.1:15445",
               false, false)

// Size of the brain task queue
SETTING_int(peloton_brain_task_queue_size,
            "Brain Task Queue Size (default: 32)",
            32,
            1, 128,
            false, false)

// Size of the brain worker pool
SETTING_int(peloton_brain_worker_pool_size,
            "Brain Worker Pool Size (default: 1)", //FS: I set this to 0 since it's not something we use?
            0, //1,
            0, 16, //1, 16,
            false, false)

//===----------------------------------------------------------------------===//
// CODEGEN
//===----------------------------------------------------------------------===//

SETTING_bool(peloton_codegen,
            "Enable code-generation for query execution (default: true)",
            false, //true,
            true, true)

SETTING_bool(peloton_codegen_interpreter,
             "Force interpretation of generated llvm code (default: false)",
             false, true, true)

SETTING_bool(peloton_print_ir_stats,
             "Print statistics on generated IR (default: false)",
             false,
             true, true)

SETTING_bool(peloton_dump_ir,
             "Enable logging of all generated IR (default: false)",
             false,
             true, true)

//===----------------------------------------------------------------------===//
// Optimizer
//===----------------------------------------------------------------------===//
SETTING_bool(peloton_predicate_push_down,
             "Enable predicate push-down optimization (default: true)",
             true,
             true, true)

SETTING_bool(peloton_hash_join_bloom_filter,
             "Enable bloom filter for hash join in codegen (default: false)",
             false,
             true, true)

SETTING_int(peloton_task_execution_timeout,
            "Maximum allowed length of time (in ms) for task "
                "execution step of optimizer, "
                "assuming one plan has been found (default 5000)",
            5000,
	    1000, 60000,
	    true, true)

//===----------------------------------------------------------------------===//
// GENERAL
//===----------------------------------------------------------------------===//

// Display configuration
SETTING_bool(peloton_display_settings,
             "Display settings (default: false)",
             false,
             true, true)