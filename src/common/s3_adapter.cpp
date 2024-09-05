/*
 *  Copyright (c) 2020 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*************************************************************************
> File Name: s3_adapter.cpp
> Author:
> Created Time: Wed Dec 19 15:19:40 2018
 ************************************************************************/

#include "src/common/s3_adapter.h"

#include <aws/core/utils/stream/PreallocatedStreamBuf.h>
#include <glog/logging.h>

#include <memory>
#include <sstream>
#include <string>
#include <utility>

#include "src/common/curve_define.h"
#include "src/common/macros.h"

#define AWS_ALLOCATE_TAG __FILE__ ":" STRINGIFY(__LINE__)

namespace curve {
namespace common {

std::once_flag s3_init_flag;
std::once_flag s3_shutdown_flag;
Aws::SDKOptions aws_sdk_options;

namespace {

// https://github.com/aws/aws-sdk-cpp/issues/1430
class PreallocatedIOStream : public Aws::IOStream {
 public:
  PreallocatedIOStream(char* buf, size_t size)
      : Aws::IOStream(new Aws::Utils::Stream::PreallocatedStreamBuf(
            reinterpret_cast<unsigned char*>(buf), size)) {}

  PreallocatedIOStream(const char* buf, size_t size)
      : PreallocatedIOStream(const_cast<char*>(buf), size) {}

  ~PreallocatedIOStream() override {
    // corresponding new in constructor
    delete rdbuf();
  }
};

// https://developer.mozilla.org/zh-CN/docs/Web/HTTP/Range_requests
Aws::String GetObjectRequestRange(uint64_t offset, uint64_t len) {
  CHECK_GT(len, 0);
  auto range = "bytes=" + std::to_string(offset) + "-" +
               std::to_string(offset + len - 1);
  return {range.data(), range.size()};
}

}  // namespace

void InitS3AdaptorOption(Configuration* conf, S3AdapterOption* s3_opt) {
  InitS3AdaptorOptionExceptS3InfoOption(conf, s3_opt);
  LOG_IF(FATAL, !conf->GetStringValue("s3.endpoint", &s3_opt->s3Address));
  LOG_IF(FATAL, !conf->GetStringValue("s3.ak", &s3_opt->ak));
  LOG_IF(FATAL, !conf->GetStringValue("s3.sk", &s3_opt->sk));
  LOG_IF(FATAL, !conf->GetStringValue("s3.bucket_name", &s3_opt->bucketName));
}

void InitS3AdaptorOptionExceptS3InfoOption(Configuration* conf,
                                           S3AdapterOption* s3_opt) {
  LOG_IF(FATAL, !conf->GetIntValue("s3.logLevel", &s3_opt->loglevel));
  LOG_IF(FATAL, !conf->GetStringValue("s3.logPrefix", &s3_opt->logPrefix));
  LOG_IF(FATAL, !conf->GetIntValue("s3.http_scheme", &s3_opt->scheme));
  LOG_IF(FATAL, !conf->GetBoolValue("s3.verify_SSL", &s3_opt->verifySsl));
  LOG_IF(FATAL,
         !conf->GetIntValue("s3.maxConnections", &s3_opt->maxConnections));
  LOG_IF(FATAL,
         !conf->GetIntValue("s3.connectTimeout", &s3_opt->connectTimeout));
  LOG_IF(FATAL,
         !conf->GetIntValue("s3.requestTimeout", &s3_opt->requestTimeout));
  LOG_IF(FATAL,
         !conf->GetIntValue("s3.asyncThreadNum", &s3_opt->asyncThreadNum));
  LOG_IF(FATAL, !conf->GetUInt64Value("s3.throttle.iopsTotalLimit",
                                      &s3_opt->iopsTotalLimit));
  LOG_IF(FATAL, !conf->GetUInt64Value("s3.throttle.iopsReadLimit",
                                      &s3_opt->iopsReadLimit));
  LOG_IF(FATAL, !conf->GetUInt64Value("s3.throttle.iopsWriteLimit",
                                      &s3_opt->iopsWriteLimit));
  LOG_IF(FATAL,
         !conf->GetUInt64Value("s3.throttle.bpsTotalMB", &s3_opt->bpsTotalMB));
  LOG_IF(FATAL,
         !conf->GetUInt64Value("s3.throttle.bpsReadMB", &s3_opt->bpsReadMB));
  LOG_IF(FATAL,
         !conf->GetUInt64Value("s3.throttle.bpsWriteMB", &s3_opt->bpsWriteMB));
  LOG_IF(FATAL, !conf->GetBoolValue("s3.useVirtualAddressing",
                                    &s3_opt->useVirtualAddressing));
  LOG_IF(FATAL, !conf->GetStringValue("s3.region", &s3_opt->region));

  if (!conf->GetUInt64Value("s3.maxAsyncRequestInflightBytes",
                            &s3_opt->maxAsyncRequestInflightBytes)) {
    LOG(WARNING) << "Not found s3.maxAsyncRequestInflightBytes in conf";
    s3_opt->maxAsyncRequestInflightBytes = 0;
  }
}

void S3Adapter::Init(const std::string& path) {
  LOG(INFO) << "Loading s3 configurations";
  conf_.SetConfigPath(path);
  LOG_IF(FATAL, !conf_.LoadConfig())
      << "Failed to open s3 config file: " << conf_.GetConfigPath();
  S3AdapterOption option;
  InitS3AdaptorOption(&conf_, &option);
  Init(option);
}

void S3Adapter::InitExceptFsS3Option(const std::string& path) {
  LOG(INFO) << "Loading s3 configurations";
  conf_.SetConfigPath(path);
  LOG_IF(FATAL, !conf_.LoadConfig())
      << "Failed to open s3 config file: " << conf_.GetConfigPath();
  S3AdapterOption option;
  InitS3AdaptorOptionExceptS3InfoOption(&conf_, &option);
  Init(option);
}

void S3Adapter::Init(const S3AdapterOption& option) {
  auto init_sdk = [&]() {
    aws_sdk_options.loggingOptions.logLevel =
        Aws::Utils::Logging::LogLevel(option.loglevel);
    aws_sdk_options.loggingOptions.defaultLogPrefix = option.logPrefix.c_str();
    Aws::InitAPI(aws_sdk_options);
  };
  std::call_once(s3_init_flag, init_sdk);

  s3Address_ = option.s3Address;
  s3Ak_ = option.ak;
  s3Sk_ = option.sk;
  bucketName_ = option.bucketName;
  clientCfg_ = Aws::New<Aws::Client::ClientConfiguration>(AWS_ALLOCATE_TAG);
  clientCfg_->scheme = Aws::Http::Scheme(option.scheme);
  clientCfg_->verifySSL = option.verifySsl;
  // clientCfg_->userAgent = conf_.GetStringValue("s3.user_agent_conf").c_str();
  // //NOLINT
  clientCfg_->userAgent = "S3 Browser";
  clientCfg_->region = option.region;
  clientCfg_->maxConnections = option.maxConnections;
  clientCfg_->connectTimeoutMs = option.connectTimeout;
  clientCfg_->requestTimeoutMs = option.requestTimeout;
  clientCfg_->endpointOverride = s3Address_;
  int async_thread_num = option.asyncThreadNum;
  LOG(INFO) << "S3Adapter init thread num = " << async_thread_num << std::endl;
  clientCfg_->executor =
      Aws::MakeShared<Aws::Utils::Threading::PooledThreadExecutor>(
          "S3Adapter.S3Client", async_thread_num);
  s3Client_ = Aws::New<Aws::S3::S3Client>(
      AWS_ALLOCATE_TAG, Aws::Auth::AWSCredentials(s3Ak_, s3Sk_), *clientCfg_,
      Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
      option.useVirtualAddressing);

  ReadWriteThrottleParams params;
  params.iopsTotal.limit = option.iopsTotalLimit;
  params.iopsRead.limit = option.iopsReadLimit;
  params.iopsWrite.limit = option.iopsWriteLimit;
  params.bpsTotal.limit = option.bpsTotalMB * kMB;
  params.bpsRead.limit = option.bpsReadMB * kMB;
  params.bpsWrite.limit = option.bpsWriteMB * kMB;

  throttle_ = new Throttle();
  throttle_->UpdateThrottleParams(params);

  inflightBytesThrottle_ = std::make_unique<AsyncRequestInflightBytesThrottle>(
      option.maxAsyncRequestInflightBytes == 0
          ? UINT64_MAX
          : option.maxAsyncRequestInflightBytes);
}

void S3Adapter::Deinit() {
  // delete s3client in s3adapter
  if (clientCfg_ != nullptr) {
    Aws::Delete<Aws::Client::ClientConfiguration>(clientCfg_);
    clientCfg_ = nullptr;
  }
  if (s3Client_ != nullptr) {
    Aws::Delete<Aws::S3::S3Client>(s3Client_);
    s3Client_ = nullptr;
  }
  if (throttle_ != nullptr) {
    delete throttle_;
    throttle_ = nullptr;
  }
  if (inflightBytesThrottle_ != nullptr) inflightBytesThrottle_.release();
}

void S3Adapter::Shutdown() {
  // one program should only call once
  auto shutdown_sdk = [&]() { Aws::ShutdownAPI(aws_sdk_options); };
  std::call_once(s3_shutdown_flag, shutdown_sdk);
}

void S3Adapter::Reinit(const S3AdapterOption& option) {
  Deinit();
  Init(option);
}

std::string S3Adapter::GetS3Ak() {
  return std::string(s3Ak_.c_str(), s3Ak_.size());
}

std::string S3Adapter::GetS3Sk() {
  return std::string(s3Sk_.c_str(), s3Sk_.size());
}

std::string S3Adapter::GetS3Endpoint() {
  return std::string(s3Address_.c_str(), s3Address_.size());
}

int S3Adapter::CreateBucket() {
  Aws::S3::Model::CreateBucketRequest request;
  request.SetBucket(bucketName_);
  Aws::S3::Model::CreateBucketConfiguration conf;
  conf.SetLocationConstraint(
      Aws::S3::Model::BucketLocationConstraint::us_east_1);
  request.SetCreateBucketConfiguration(conf);
  auto response = s3Client_->CreateBucket(request);
  if (response.IsSuccess()) {
    return 0;
  } else {
    LOG(ERROR) << "CreateBucket error:" << bucketName_ << "--"
               << response.GetError().GetExceptionName()
               << response.GetError().GetMessage();
    return -1;
  }
}

int S3Adapter::DeleteBucket() {
  Aws::S3::Model::DeleteBucketRequest request;
  request.SetBucket(bucketName_);
  auto response = s3Client_->DeleteBucket(request);
  if (response.IsSuccess()) {
    return 0;
  } else {
    LOG(ERROR) << "DeleteBucket error:" << bucketName_ << "--"
               << response.GetError().GetExceptionName()
               << response.GetError().GetMessage();
    return -1;
  }
}

bool S3Adapter::BucketExist() {
  Aws::S3::Model::HeadBucketRequest request;
  request.SetBucket(bucketName_);
  auto response = s3Client_->HeadBucket(request);
  if (response.IsSuccess()) {
    return true;
  } else {
    LOG(ERROR) << "HeadBucket error:" << bucketName_ << "--"
               << response.GetError().GetExceptionName()
               << response.GetError().GetMessage();
    return false;
  }
}

int S3Adapter::PutObject(const Aws::String& key, const char* buffer,
                         const size_t buffer_size) {
  Aws::S3::Model::PutObjectRequest request;
  request.SetBucket(bucketName_);
  request.SetKey(key);

  request.SetBody(Aws::MakeShared<PreallocatedIOStream>(AWS_ALLOCATE_TAG,
                                                        buffer, buffer_size));

  if (throttle_) {
    throttle_->Add(false, buffer_size);
  }

  auto response = s3Client_->PutObject(request);
  if (response.IsSuccess()) {
    return 0;
  } else {
    LOG(ERROR) << "PutObject error, bucket: " << bucketName_ << ", key: " << key
               << response.GetError().GetExceptionName()
               << response.GetError().GetMessage();
    return -1;
  }
}

int S3Adapter::PutObject(const Aws::String& key, const std::string& data) {
  return PutObject(key, data.data(), data.size());
}

void S3Adapter::PutObjectAsync(std::shared_ptr<PutObjectAsyncContext> context) {
  Aws::S3::Model::PutObjectRequest request;
  request.SetBucket(bucketName_);
  request.SetKey(Aws::String{context->key.c_str(), context->key.size()});

  request.SetBody(Aws::MakeShared<PreallocatedIOStream>(
      AWS_ALLOCATE_TAG, context->buffer, context->bufferSize));

  auto origin_callback = context->cb;
  auto wrapper_callback =
      [this,
       origin_callback](const std::shared_ptr<PutObjectAsyncContext>& ctx) {
        inflightBytesThrottle_->OnComplete(ctx->bufferSize);
        ctx->cb = origin_callback;
        ctx->cb(ctx);
      };

  Aws::S3::PutObjectResponseReceivedHandler handler =
      [context](const Aws::S3::S3Client* /*client*/,
                const Aws::S3::Model::PutObjectRequest& /*request*/,
                const Aws::S3::Model::PutObjectOutcome& response,
                const std::shared_ptr<const Aws::Client::AsyncCallerContext>&
                    aws_ctx) {
        std::shared_ptr<PutObjectAsyncContext> ctx =
            std::const_pointer_cast<PutObjectAsyncContext>(
                std::dynamic_pointer_cast<const PutObjectAsyncContext>(
                    aws_ctx));

        LOG_IF(ERROR, !response.IsSuccess())
            << "PutObjectAsync error: "
            << response.GetError().GetExceptionName()
            << "message: " << response.GetError().GetMessage()
            << "resend: " << ctx->key;

        ctx->retCode = (response.IsSuccess() ? 0 : -1);
        ctx->cb(ctx);
      };

  if (throttle_) {
    throttle_->Add(false, context->bufferSize);
  }

  inflightBytesThrottle_->OnStart(context->bufferSize);
  context->cb = std::move(wrapper_callback);
  s3Client_->PutObjectAsync(request, handler, context);
}

int S3Adapter::GetObject(const Aws::String& key, std::string* data) {
  Aws::S3::Model::GetObjectRequest request;
  request.SetBucket(bucketName_);
  request.SetKey(key);
  std::stringstream ss;
  if (throttle_) {
    throttle_->Add(true, 1);
  }
  auto response = s3Client_->GetObject(request);
  if (response.IsSuccess()) {
    ss << response.GetResult().GetBody().rdbuf();
    *data = ss.str();
    return 0;
  } else {
    LOG(ERROR) << "GetObject error: " << response.GetError().GetExceptionName()
               << response.GetError().GetMessage();
    return -1;
  }
}

int S3Adapter::GetObject(const std::string& key, char* buf, off_t offset,
                         size_t len) {
  Aws::S3::Model::GetObjectRequest request;
  request.SetBucket(bucketName_);
  request.SetKey(Aws::String{key.c_str(), key.size()});
  request.SetRange(GetObjectRequestRange(offset, len));

  request.SetResponseStreamFactory([buf, len]() {
    return Aws::New<PreallocatedIOStream>(AWS_ALLOCATE_TAG, buf, len);
  });

  if (throttle_) {
    throttle_->Add(true, len);
  }
  auto response = s3Client_->GetObject(request);
  if (response.IsSuccess()) {
    return 0;
  } else {
    LOG(ERROR) << "GetObject error: " << response.GetError().GetExceptionName()
               << response.GetError().GetMessage();
    return -1;
  }
}

void S3Adapter::GetObjectAsync(std::shared_ptr<GetObjectAsyncContext> context) {
  Aws::S3::Model::GetObjectRequest request;
  request.SetBucket(bucketName_);
  request.SetKey(Aws::String{context->key.c_str(), context->key.size()});
  request.SetRange(GetObjectRequestRange(context->offset, context->len));

  request.SetResponseStreamFactory([context]() {
    return Aws::New<PreallocatedIOStream>(AWS_ALLOCATE_TAG, context->buf,
                                          context->len);
  });

  auto origin_callback = context->cb;
  auto wrapper_callback =
      [this, origin_callback](
          const S3Adapter* /*adapter*/,
          const std::shared_ptr<GetObjectAsyncContext>& ctx) {
        inflightBytesThrottle_->OnComplete(ctx->len);
        ctx->cb = origin_callback;
        ctx->cb(this, ctx);
      };

  Aws::S3::GetObjectResponseReceivedHandler handler =
      [this](const Aws::S3::S3Client* /*client*/,
             const Aws::S3::Model::GetObjectRequest& /*request*/,
             const Aws::S3::Model::GetObjectOutcome& response,
             const std::shared_ptr<const Aws::Client::AsyncCallerContext>&
                 aws_ctx) {
        std::shared_ptr<GetObjectAsyncContext> ctx =
            std::const_pointer_cast<GetObjectAsyncContext>(
                std::dynamic_pointer_cast<const GetObjectAsyncContext>(
                    aws_ctx));

        LOG_IF(ERROR, !response.IsSuccess())
            << "GetObjectAsync error: "
            << response.GetError().GetExceptionName()
            << response.GetError().GetMessage();
        ctx->actualLen = response.GetResult().GetContentLength();
        ctx->retCode = (response.IsSuccess() ? 0 : -1);
        ctx->cb(this, ctx);
      };

  if (throttle_) {
    throttle_->Add(true, context->len);
  }

  inflightBytesThrottle_->OnStart(context->len);
  context->cb = std::move(wrapper_callback);
  s3Client_->GetObjectAsync(request, handler, context);
}

bool S3Adapter::ObjectExist(const Aws::String& key) {
  Aws::S3::Model::HeadObjectRequest request;
  request.SetBucket(bucketName_);
  request.SetKey(key);
  auto response = s3Client_->HeadObject(request);
  if (response.IsSuccess()) {
    return true;
  } else {
    LOG(ERROR) << "HeadObject error:" << bucketName_ << "--" << key << "--"
               << response.GetError().GetExceptionName()
               << response.GetError().GetMessage();
    return false;
  }
}

int S3Adapter::DeleteObject(const Aws::String& key) {
  Aws::S3::Model::DeleteObjectRequest request;
  request.SetBucket(bucketName_);
  request.SetKey(key);
  auto response = s3Client_->DeleteObject(request);
  if (response.IsSuccess()) {
    return 0;
  } else {
    LOG(ERROR) << "DeleteObject error:" << bucketName_ << "--" << key << "--"
               << response.GetError().GetExceptionName()
               << response.GetError().GetMessage();
    return -1;
  }
}

int S3Adapter::DeleteObjects(const std::list<Aws::String>& key_list) {
  Aws::S3::Model::DeleteObjectsRequest delete_objects_request;
  Aws::S3::Model::Delete delete_objects;
  for (const auto& key : key_list) {
    Aws::S3::Model::ObjectIdentifier obj_ident;
    obj_ident.SetKey(key);
    delete_objects.AddObjects(obj_ident);
  }

  delete_objects.SetQuiet(false);
  delete_objects_request.WithBucket(bucketName_).WithDelete(delete_objects);
  auto response = s3Client_->DeleteObjects(delete_objects_request);
  if (response.IsSuccess()) {
    for (const auto& del : response.GetResult().GetDeleted()) {
      LOG(INFO) << "delete ok : " << del.GetKey();
    }

    for (const auto& err : response.GetResult().GetErrors()) {
      LOG(WARNING) << "delete err : " << err.GetKey() << " --> "
                   << err.GetMessage();
    }

    if (response.GetResult().GetErrors().size() != 0) {
      return -1;
    }

    return 0;
  } else {
    LOG(ERROR) << response.GetError().GetMessage() << " failed, "
               << delete_objects_request.SerializePayload();
    return -1;
  }
  return 0;
}

Aws::String S3Adapter::MultiUploadInit(const Aws::String& key) {
  Aws::S3::Model::CreateMultipartUploadRequest request;
  request.WithBucket(bucketName_).WithKey(key);
  auto response = s3Client_->CreateMultipartUpload(request);
  if (response.IsSuccess()) {
    return response.GetResult().GetUploadId();
  } else {
    LOG(ERROR) << "CreateMultipartUploadRequest error: "
               << response.GetError().GetMessage();
    return "";
  }
}

Aws::S3::Model::CompletedPart S3Adapter::UploadOnePart(
    const Aws::String& key, const Aws::String& upload_id, int part_num,
    int part_size, const char* buf) {
  Aws::S3::Model::UploadPartRequest request;
  request.SetBucket(bucketName_);
  request.SetKey(key);
  request.SetUploadId(upload_id);
  request.SetPartNumber(part_num);
  request.SetContentLength(part_size);

  request.SetBody(
      Aws::MakeShared<PreallocatedIOStream>(AWS_ALLOCATE_TAG, buf, part_size));

  if (throttle_) {
    throttle_->Add(false, part_size);
  }
  auto result = s3Client_->UploadPart(request);
  if (result.IsSuccess()) {
    return Aws::S3::Model::CompletedPart()
        .WithETag(result.GetResult().GetETag())
        .WithPartNumber(part_num);
  } else {
    return Aws::S3::Model::CompletedPart()
        .WithETag("errorTag")
        .WithPartNumber(-1);
  }
}

int S3Adapter::CompleteMultiUpload(
    const Aws::String& key, const Aws::String& upload_id,
    const Aws::Vector<Aws::S3::Model::CompletedPart>& cp_v) {
  Aws::S3::Model::CompleteMultipartUploadRequest request;
  request.WithBucket(bucketName_);
  request.SetKey(key);
  request.SetUploadId(upload_id);
  request.SetMultipartUpload(
      Aws::S3::Model::CompletedMultipartUpload().WithParts(cp_v));
  auto response = s3Client_->CompleteMultipartUpload(request);
  if (response.IsSuccess()) {
    return 0;
  } else {
    LOG(ERROR) << "CompleteMultiUpload error: "
               << response.GetError().GetMessage();
    this->AbortMultiUpload(key, upload_id);
    return -1;
  }
}

int S3Adapter::AbortMultiUpload(const Aws::String& key,
                                const Aws::String& upload_id) {
  Aws::S3::Model::AbortMultipartUploadRequest request;
  request.WithBucket(bucketName_);
  request.SetKey(key);
  request.SetUploadId(upload_id);
  auto response = s3Client_->AbortMultipartUpload(request);
  if (response.IsSuccess()) {
    return 0;
  } else {
    LOG(ERROR) << "AbortMultiUpload error: "
               << response.GetError().GetMessage();
    return -1;
  }
}

void S3Adapter::AsyncRequestInflightBytesThrottle::OnStart(uint64_t len) {
  std::unique_lock<std::mutex> lock(mtx_);
  while (inflightBytes_ + len > maxInflightBytes_) {
    cond_.wait(lock);
  }

  inflightBytes_ += len;
}

void S3Adapter::AsyncRequestInflightBytesThrottle::OnComplete(uint64_t len) {
  std::unique_lock<std::mutex> lock(mtx_);
  inflightBytes_ -= len;
  cond_.notify_all();
}

void S3Adapter::SetS3Option(const S3InfoOption& fs_s3_opt) {
  s3Address_ = fs_s3_opt.s3Address;
  s3Ak_ = fs_s3_opt.ak;
  s3Sk_ = fs_s3_opt.sk;
  bucketName_ = fs_s3_opt.bucketName;
}

}  // namespace common
}  // namespace curve
