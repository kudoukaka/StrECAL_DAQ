#ifndef RoestiDash_h
#define RoestiDash_h

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

class RoestiDash : public FairMQDevice {
public:

    struct OptionKey {
        static constexpr std::string_view InputChannelName{"in"};
        static constexpr std::string_view Multipart{"multipart"};
    };

    RoestiDash() = default;
    RoestiDash(const RoestiDash&) = delete;
    RoestiDash &operator=(const RoestiDash&) = delete;
    ~RoestiDash() = default;

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