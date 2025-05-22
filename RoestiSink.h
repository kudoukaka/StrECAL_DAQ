#ifndef RoestiSink_h
#define RoestiSink_h

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <fstream>
 #include <vector>
#include <unordered_map>
#include <unordered_set>

//#include "emulator/AmQTdcData.h"
#include <iostream>

#if __has_include(<fairmq/Device.h>)
#include <fairmq/Device.h> // since v1.4.34
#else
#include <fairmq/FairMQDevice.h>
#endif

class RoestiSink : public FairMQDevice {
public:

    struct OptionKey {
        static constexpr std::string_view InputChannelName{"in"};
        static constexpr std::string_view Multipart{"multipart"};
    };

    RoestiSink() = default;
    RoestiSink(const RoestiSink&) = delete;
    RoestiSink &operator=(const RoestiSink&) = delete;
    ~RoestiSink() = default;

    struct STFBuffer {
      FairMQParts parts;
      std::chrono::steady_clock::time_point start;
    };

private:
    bool HandleData(FairMQMessagePtr &msg, int index);
    bool HandleMultipartData(FairMQParts &msgParts, int index);
    void Init() override;
    void InitTask() override;
    void PostRun() override;
    void PreRun() override;

    std::string fInputChannelName;
    uint64_t fNumMessages{0};
    uint64_t TotalLength(const FairMQParts& parts);

    //simple send
    uint64_t fMaxIterations = 0;
    uint64_t fNumIterations = 0;

    std::unordered_map<uint32_t, std::vector<STFBuffer>> fTFBuffer;

    bool fopened;
    std::ofstream fstr;

};


#endif
// #ifndef RoestiSink_h
// #define RoestiSink_h

// #include <string>
// #include <string_view>
// #include <cstdint>
// #include <chrono>

// #include <vector>
// #include <unordered_map>
// #include <unordered_set>

// #include <fairmq/Device.h>
// // #if __has_include(<fairmq/Device.h>)
// // #include <fairmq/Device.h> // since v1.4.34
// // #else
// // #include <fairmq/FairMQDevice.h>
// // #endif

// // #include "uhbook.cxx"
// // #include "RedisDataStore.h"
// // #include "Slowdashify.h"


// // class RoestiSink : public FairMQDevice 
// class RoestiSink : public fair::mq::Device
// {
// public:
//     struct OptionKey {
//         // static constexpr std::string_view NumSource            {"num-source"};
//         static constexpr std::string_view BufferTimeoutInMs    {"buffer-timeout"};
//         static constexpr std::string_view InputChannelName     {"in-chan-name"};
//         static constexpr std::string_view OutputChannelName    {"out-chan-name"};
//         static constexpr std::string_view DQMChannelName       {"dqm-chan-name"};      
//         static constexpr std::string_view PollTimeout          {"poll-timeout"};
//         static constexpr std::string_view DecimatorChannelName {"decimator-chan-name"};
//         static constexpr std::string_view DecimationFactor     {"decimation-factor"};
//         static constexpr std::string_view DecimationOffset     {"decimation-offset"};
//         static constexpr std::string_view RedisUrl             {"redis-url"};
//         static constexpr std::string_view RedisObjUrl          {"redis-obj-url"};
//         static constexpr std::string_view Multipart{"multipart"};
//     };

//     struct STFBuffer {
//         FairMQParts parts;
//         std::chrono::steady_clock::time_point start;
//     };

//     RoestiSink();
//     RoestiSink(const RoestiSink&)            = delete;
//     RoestiSink& operator=(const RoestiSink&) = delete;
//     ~RoestiSink() = default;


// protected:
//     bool ConditionalRun() override;
//     void Init() override;
//     void InitTask() override;
//     void PostRun() override;
//     void PreRun() override;
//     bool HandleData(FairMQMessagePtr &msg, int index);
//     bool HandleMultipartData(FairMQParts &msgParts, int index);

// private:
//     int fNumSource {0};
//     int fBufferTimeoutInMs {10000};
//     std::string fInputChannelName;
//     std::string fOutputChannelName;
//     std::string fDQMChannelName;  
//     std::string fDecimatorChannelName;
//     int fNumDestination   {0};
//     uint32_t fDirection   {0}; // used to determine the sub-socket index
//     uint32_t fDecimationFactor {0};
//     uint32_t fDecimationOffset {0};
//     int fDecimatorNumberOfConnectedPeers {0};
//     int fPollTimeoutMS    {0};
//     uint64_t fNumSend {0};

//     std::unordered_map<uint32_t, std::vector<STFBuffer>> fTFBuffer;
//     //std::unordered_set<uint64_t> fDiscarded;

//     // std::string fRedisUrl;
//     // std::unique_ptr<RedisDataStore> fDb;
//     // std::string fKeyPrefix;
//     // std::string fRedisObjUrl;
//     // std::unique_ptr<RedisDataStore> fObjDb;
//     // std::string fKeyObjPrefix;

//     // UH1Book *fHId;
//     uint64_t fNumMessages{0};
//     uint64_t TotalLength(const FairMQParts& parts);
//     bool fopened;
//     std::ofstream fstr;

// };

// #endif
