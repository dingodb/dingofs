// Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
#ifndef DINGOFS_TOOLS_MDS_CLI_OUTPUT_H_
#define DINGOFS_TOOLS_MDS_CLI_OUTPUT_H_

#include <google/protobuf/message.h>

#include <cstdint>
#include <string>
#include <vector>

namespace dingofs::mds::client {

enum class OutputFormat { kPretty, kJson };
enum class ColorMode { kNever, kAuto, kAlways };

struct OutputConfig {
  OutputFormat format{OutputFormat::kPretty};
  ColorMode color{ColorMode::kNever};
  bool verbose{false};
};

void SetOutputConfig(OutputConfig config);
const OutputConfig& GetOutputConfig();
int GetOutputExitCode();

// Render a command result. The JSON shape is intentionally stable and does
// not expose protobuf ShortDebugString output.
void PrintSuccess(
    const std::string& command, const std::string& summary,
    const std::vector<std::pair<std::string, std::string>>& fields = {});
void PrintFailure(const std::string& command, const std::string& code,
                  const std::string& message);
void PrintMessage(const std::string& command, const std::string& summary,
                  const google::protobuf::Message& message);
void PrintTitle(const std::string& title);
void PrintFields(
    const std::vector<std::pair<std::string, std::string>>& fields);

}  // namespace dingofs::mds::client

#endif  // DINGOFS_TOOLS_MDS_CLI_OUTPUT_H_
