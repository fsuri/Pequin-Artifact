//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// peloton.cpp
//
// Identification: src/main/peloton/peloton.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>

#include <gflags/gflags.h>
#include "../common/init.h"
#include "../common/logger.h"
#include "../network/peloton_server.h"
#include "../settings/settings_manager.h"
#include "brain/brain.h"

// For GFlag's built-in help message flag
DECLARE_bool(help);

int RunPelotonServer() {
  int return_code = 0;
  try {
    // Setup
    peloton_peloton::PelotonInit::Initialize();

    peloton_peloton::network::PelotonServer peloton_server;

    peloton_peloton::network::PelotonServer::LoadSSLFileSettings();
    peloton_peloton::network::PelotonServer::SSLInit();

    peloton_server.SetupServer().ServerLoop();
  } catch (peloton_peloton::ConnectionException &exception) {
    //log error message and mark failure
    peloton_peloton::LOG_ERROR("Cannot start server. Failure detail : %s\n", exception.GetMessage().c_str());
    return_code = EXIT_FAILURE;
  }

  // Teardown
  peloton_peloton::PelotonInit::Shutdown();
  return return_code;
}


int RunPelotonBrain() {
  // TODO(tianyu): boot up other peloton resources as needed here
  peloton_peloton::brain::Brain brain;
  evthread_use_pthreads();
  // TODO(tianyu): register jobs here
  struct timeval one_second;
  one_second.tv_sec = 1;
  one_second.tv_usec = 0;

  auto example_task = [](peloton_peloton::brain::BrainEnvironment *) {
    // TODO(tianyu): Replace with real address
    capnp::EzRpcClient client("localhost:15445");
    PelotonService::Client peloton_service = client.getMain<PelotonService>();
    auto request = peloton_service.createIndexRequest();
    request.getRequest().setIndexKeys(42);
    auto response = request.send().wait(client.getWaitScope());
  };

  brain.RegisterJob<peloton_peloton::brain::SimpleBrainJob>(&one_second, "test", example_task);
  brain.Run();
  return 0;
}

int main(int argc, char *argv[]) {

  // Parse the command line flags
  ::google::ParseCommandLineNonHelpFlags(&argc, &argv, true);

  // If "-h" or "-help" is passed in, set up the help messages.
  if (FLAGS_help) {
    ::google::SetUsageMessage("Usage Info: \n");
    ::google::HandleCommandLineHelpFlags();
  }

  try {
    // Print settings
    if (peloton_peloton::settings::SettingsManager::GetBool(
      peloton_peloton::settings::SettingId::display_settings)) {
      auto &settings = peloton_peloton::settings::SettingsManager::GetInstance();
      settings.ShowInfo();
    }
  } catch (peloton_peloton::SettingsException &exception) {
    peloton_peloton::LOG_ERROR("Cannot load settings. Failed with %s\n", exception.GetMessage().c_str());
    return EXIT_FAILURE; // TODO: Use an enum with exit error codes
  }

  int exit_code = 0;
  if (peloton_peloton::settings::SettingsManager::GetBool(
      peloton_peloton::settings::SettingId::brain))
    exit_code =  RunPelotonBrain();
  else
    exit_code = RunPelotonServer();
  return exit_code;
}
