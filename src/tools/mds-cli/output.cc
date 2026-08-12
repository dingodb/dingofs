// Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
#include "tools/mds-cli/output.h"

#include <google/protobuf/util/json_util.h>
#include <json/reader.h>
#include <json/value.h>
#include <json/writer.h>
#include <unistd.h>

#include <algorithm>
#include <iostream>
#include <memory>
#include <utility>

namespace dingofs::mds::client {
namespace {
OutputConfig g_config;
int g_exit_code = 0;

bool UseColor() {
  if (g_config.color == ColorMode::kAlways) return true;
  return g_config.color == ColorMode::kAuto && isatty(STDOUT_FILENO);
}

std::string JsonString(const Json::Value& value) {
  Json::StreamWriterBuilder builder;
  builder["indentation"] = "  ";
  return Json::writeString(builder, value);
}

Json::Value BaseResult(const std::string& command, bool success) {
  Json::Value result(Json::objectValue);
  result["success"] = success;
  result["command"] = command;
  return result;
}
}  // namespace

void SetOutputConfig(OutputConfig config) {
  g_config = std::move(config);
  g_exit_code = 0;
}
const OutputConfig& GetOutputConfig() { return g_config; }
int GetOutputExitCode() { return g_exit_code; }

void PrintSuccess(
    const std::string& command, const std::string& summary,
    const std::vector<std::pair<std::string, std::string>>& fields) {
  if (g_config.format == OutputFormat::kJson) {
    auto result = BaseResult(command, true);
    Json::Value data(Json::objectValue);
    if (!summary.empty()) data["summary"] = summary;
    for (const auto& [key, value] : fields) data[key] = value;
    result["data"] = data;
    std::cout << JsonString(result) << '\n';
    return;
  }

  const std::string mark = UseColor() ? "\033[32m✓\033[0m" : "✓";
  std::cout << mark << ' ' << summary << '\n';
  if (!fields.empty()) {
    PrintTitle(command);
    PrintFields(fields);
  }
}

void PrintMessage(const std::string& command, const std::string& summary,
                  const google::protobuf::Message& message) {
  std::string json;
  google::protobuf::util::JsonPrintOptions options;
  options.preserve_proto_field_names = true;
  if (!google::protobuf::util::MessageToJsonString(message, &json, options)
           .ok()) {
    PrintFailure(command, "FORMAT_ERROR", "unable to serialize response");
    return;
  }

  if (g_config.format == OutputFormat::kJson) {
    Json::CharReaderBuilder reader_builder;
    Json::Value data;
    std::string errors;
    std::unique_ptr<Json::CharReader> reader(reader_builder.newCharReader());
    if (!reader->parse(json.data(), json.data() + json.size(), &data,
                       &errors)) {
      PrintFailure(command, "FORMAT_ERROR", errors);
      return;
    }
    auto result = BaseResult(command, true);
    result["data"] = data;
    std::cout << JsonString(result) << '\n';
    return;
  }

  PrintSuccess(command, summary);
  PrintTitle("Details");
  std::cout << json;
  if (json.empty() || json.back() != '\n') std::cout << '\n';
}

void PrintFailure(const std::string& command, const std::string& code,
                  const std::string& message) {
  g_exit_code = 1;
  if (g_config.format == OutputFormat::kJson) {
    auto result = BaseResult(command, false);
    result["error"]["code"] = code;
    result["error"]["message"] = message;
    std::cout << JsonString(result) << '\n';
    return;
  }

  const std::string mark = UseColor() ? "\033[31m✗\033[0m" : "✗";
  std::cout << mark << ' ' << command << " failed: " << message << '\n';
}

void PrintTitle(const std::string& title) {
  std::cout << '\n' << title << '\n';
  std::cout << std::string(title.size(), '-') << '\n';
}

void PrintFields(
    const std::vector<std::pair<std::string, std::string>>& fields) {
  size_t width = 0;
  for (const auto& [key, value] : fields) width = std::max(width, key.size());
  for (const auto& [key, value] : fields) {
    std::cout << "  " << key << std::string(width - key.size(), ' ') << "  "
              << value << '\n';
  }
}
}  // namespace dingofs::mds::client
