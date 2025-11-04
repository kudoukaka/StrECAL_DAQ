#include <chrono>
#include <ios>
#include <ostream>
#include <thread>
#include <vector>
#include <numeric>
#include <iostream>
#include <fstream>
#include <sstream>
#include <iomanip>
#include <functional>
#include <thread>
#include <cassert>
#include <algorithm>
#include <string>


// #include "TBufferFile.h"
#include "TFile.h"
#include "TTree.h"
#include "TLeaf.h"
#include "TObject.h"
#include "TAxis.h" 
#include <TGraph.h>
#include <TCanvas.h>
#include <arpa/inet.h> // For ntohs()
// #include <hiredis/hiredis.h>

#include <fairmq/runFairMQDevice.h>
// #include <fairmq/runDevice.h>
// #include <fairmq/Device.h>

#include "SubTimeFrameHeader.h"
#include "roesti.h"
#include "RoestiDash.h"
#include "RedisDataStore.h"
#include "Slowdashify.h"

#include "hexdump.cxx"

// static constexpr std::string_view MyClass{"RoestiDash"};

static RedisDataStore* gDB = nullptr;

namespace bpo = boost::program_options;

//_____________________________________________________________________________
void addCustomOptions(bpo::options_description &options)
{
    using opt = RoestiDash::OptionKey;
    options.add_options()
    // (opt::InputChannelName.data(), bpo::value<std::string>()->default_value(opt::InputChannelName.data()), "Name of input channel\n")
    (opt::Multipart.data(), bpo::value<std::string>()->default_value("true"), "Handle multipart message\n")

    // (opt::BufferTimeoutInMs.data(),    bpo::value<std::string>()->default_value("10000"),     "Buffer timeout in milliseconds")
    (opt::InputChannelName.data(),     bpo::value<std::string>()->default_value("in"),        "Name of the input channel");
    // (opt::OutputChannelName.data(),    bpo::value<std::string>()->default_value("out"),       "Name of the output channel") 
    // (opt::DQMChannelName.data(),       bpo::value<std::string>()->default_value("dqm"),       "Name of the data quality monitoring channel")      
    // (opt::PollTimeout.data(),          bpo::value<std::string>()->default_value("0"),         "Timeout (in msec) of polling")
    // (opt::DecimatorChannelName.data(), bpo::value<std::string>()->default_value("decimator"), "Name of the decimated output channel")
    // (opt::DecimationFactor.data(),     bpo::value<std::string>()->default_value("0"),         "Decimation factor for decimated output channel")
    // (opt::DecimationOffset.data(),     bpo::value<std::string>()->default_value("0"),         "Decimation offset for decimated output channel")
    // (opt::RedisUrl.data(),             bpo::value<std::string>()->default_value("tcp://127.0.0.1:6379/1"), "URL of redis server and db number")
    // (opt::RedisObjUrl.data(),          bpo::value<std::string>()->default_value("tcp://127.0.0.1:6379/3"), "URL of redis server and db number for histogram");
}



//_____________________________________________________________________________

FairMQDevicePtr getDevice(const FairMQProgOptions &)
{
    return new RoestiDash;
}

//_____________________________________________________________________________
void PrintConfig(const fair::mq::ProgOptions* config, std::string_view name, std::string_view funcname)
{
    auto c = config->GetPropertiesAsStringStartingWith(name.data());
    std::ostringstream ss;
    ss << funcname << "\n\t " << name << "\n";
    for (const auto &[k, v] : c) {
        ss << "\t key = " << k << ", value = " << v << "\n";
    }
    LOG(debug) << ss.str();
}

//_____________________________________________________________________________
bool RoestiDash::HandleData(FairMQMessagePtr &msg, int index)
{
    // const auto ptr = reinterpret_cast<unsigned char*>(msg->GetData());
    // //  std::string s(ptr, ptr+msg->GetSize());
    // //  LOG(debug) << __FUNCTION__ << " received = " << s << " [" << index << "] " << fNumMessages;

    // LOG(debug) << __FUNCTION__ << " received = " << " [" << index << "] " << fNumMessages;


    //---Handle incoming data
    LOG(info) << "Received: \""
			<< std::string(static_cast<char*>(msg->GetData()), msg->GetSize())
			<< "\"";

    return true;
}

//_____________________________________________________________________________
uint64_t RoestiDash::TotalLength(const FairMQParts& parts)
{
    auto & c = const_cast<FairMQParts&>(parts);

    return std::accumulate(c.begin(), c.end(), static_cast<uint64_t>(0),
    [](uint64_t init, auto& m) -> uint64_t {
        return (!m) ? init : init + m->GetSize();
    });
}

//_____________________________________________________________________________
// ---------- Pedestal readout 
const int NUM_CHANNELS = 18;
const int NUM_SAMPLE = 1024;

int pedestal[NUM_CHANNELS][NUM_SAMPLE] = {{0}};

bool LoadPedestalForBoard(const std::string& filename)
{

   std::ifstream file(filename);
    if (!file.is_open()) {
        std::cerr << "Failed to open pedestal file: " << filename << '\n';
        return false;
    }

    std::string line;
    int line_num = 0;

    while (std::getline(file, line)) {
        ++line_num;
        if (line.empty() || line[0] == '#') continue;

        std::istringstream ss(line);
        std::string token;
        int ch, sample, value;

        if (!std::getline(ss, token, ',')) continue;
        ch = std::stoi(token);

        if (!std::getline(ss, token, ',')) continue;
        sample = std::stoi(token);

        if (!std::getline(ss, token, ',')) continue;
        value = std::stoi(token);

        if (ch < 0 || ch >= NUM_CHANNELS || sample < 0 || sample >= NUM_SAMPLE) {
            std::cerr << "Invalid pedestal entry at line " << line_num
                      << ": channel=" << ch << ", sample=" << sample << '\n';
            continue;
        }

        pedestal[ch][sample] = value;
    }

    return true;
}

constexpr double SAMPLE_PERIOD = 1e-6; // 1 MHz sampling
// Convert waveform to JSON
// std::string makeWaveformJSON(const std::vector<double>& y) {
//     // std::vector<double> x(NUM_SAMPLE);
//     // for (int i = 0; i < NUM_SAMPLE; ++i)
//     //     x[i] = i * SAMPLE_PERIOD;
//     // json j;
//     // j["type"] = "waveform";
//     // j["x"] = x;
//     // j["y"] = y;
//     // return j.dump();
// }

// redisContext* redisCtx = nullptr;
// void InitTask() override {
//         redisCtx = redisConnect("127.0.0.1", 6379);
//         if (!redisCtx || redisCtx->err) {
//             std::cerr << "Redis connection failed: "
//                       << (redisCtx ? redisCtx->errstr : "unknown error") << std::endl;
//             if (redisCtx) redisFree(redisCtx);
//             redisCtx = nullptr;
//             throw std::runtime_error("Cannot connect to Redis");
//         }
//         std::cout << "Connected to Redis" << std::endl;
//     }


//_____________________________________________________________________________
bool RoestiDash::HandleMultipartData(FairMQParts &msgParts, int index)
{

    if (!gDB) {
        gDB = new RedisDataStore("tcp://127.0.0.1:6379/3");
    }

    auto stfHeader = reinterpret_cast<SubTimeFrame::Header*>(msgParts.At(0)->GetData());
    auto stfId     = stfHeader->timeFrameId;

    char *receiveBuf = reinterpret_cast<char* >(msgParts.At(1)->GetData());


    // struct Roesti::RoestiHeader *rhead = reinterpret_cast<struct Roesti::RoestiHeader *>(receiveBuf);
    Roesti::RoestiHeader* rhead = reinterpret_cast<Roesti::RoestiHeader*>(receiveBuf);
    unsigned int packets_full_length = rhead->length;

    char* datapackets = receiveBuf + sizeof(Roesti::RoestiHeader);
    unsigned int packet_read = 0;
    int packet_index = 0;

    uint32_t event_id = rhead->event_num;


    bool channelReceived[NUM_CHANNELS] = {false};
    int channels_received_count = 0;

    // Vectors to hold ADC data for each channel
    std::vector<std::vector<double>> adcData(NUM_CHANNELS);
    std::vector<std::vector<double>> adcTime(NUM_CHANNELS); // time sample index

    std::vector<float> cal_mv_channels[NUM_CHANNELS];

    while(packet_read < packets_full_length){

        //----------auto packet size definition
        auto* rdpackhead = reinterpret_cast<Roesti::DataPacketHeader*>(datapackets);
        auto* rdpackdata = reinterpret_cast<uint8_t*>(datapackets + sizeof(Roesti::DataPacketHeader));
        // auto* rdpackdata = reinterpret_cast<Roesti::DataPacketData*>(datapackets + sizeof(Roesti::DataPacketHeader));
        auto* rdpackfooter = reinterpret_cast<Roesti::DataPacketFooter*>(datapackets + sizeof(Roesti::DataPacketHeader) + rdpackhead->data_length);

        uint8_t channel = rdpackhead->channel_num;
        uint16_t dataLength = rdpackhead->data_length;
        uint16_t stop_num = rdpackhead->stop_num;


        if (channel >= NUM_CHANNELS) {
            std::cerr << "Invalid channel number: " << static_cast<int>(channel) << std::endl;
            break;
        }

        size_t num_samples = dataLength / 2;
        if (num_samples > NUM_SAMPLE) num_samples = NUM_SAMPLE;

        cal_mv_channels[channel].clear();
        cal_mv_channels[channel].reserve(num_samples);


        for (int sample = 0; sample < num_samples; sample++) {

            // Extract 12-bit ADC data (4 bits are CRC code. Low-order 12 bits are the data)
            // uint16_t raw_adc = *(reinterpret_cast<uint16_t*>(rdpackdata + sample * 2)) & 0x0FFF;

            // Safe read of 16-bit sample to avoid alignment/endianness issues
            uint16_t raw_word = 0;
            memcpy(&raw_word, rdpackdata + sample * 2, sizeof(raw_word));

            uint16_t raw_adc = raw_word & 0x0FFF;

            // std::cout << "raw adc: " << static_cast<int>(raw_adc) << std::endl;
            // std::cout << "raw word" << static_cast<int>(raw_word) << std::endl;

            // mapping of capacitor number
            int capacitor_number = static_cast<int>((sample + stop_num) % 1024);
            // guard against invalid pedestal indexing if necessary
            // if (capacitor_number < 0 || capacitor_number >= 1024) capacitor_number = capacitor_number % 1024;

            // Raw value in mV
            float raw_mv = static_cast<float>(raw_adc) / 2048.0 * 1000.0;

            // If pedestal has channel/capacitor layout:
            int pedestal_value = pedestal[channel][capacitor_number];
            int corrected_adc = static_cast<int>(raw_adc) - pedestal_value;

            float cal_mv = static_cast<float>(corrected_adc) / 2048.0 * 1000.0;


            //redis push
            cal_mv_channels[channel].push_back(cal_mv);

        }

        channelReceived[channel] = true;
        channels_received_count++;

      
        size_t packet_size = sizeof(Roesti::DataPacketHeader) + dataLength + sizeof(Roesti::DataPacketFooter);
        packet_read += packet_size;
        datapackets += packet_size;
        

        // Check if all channels have arrived
        if (channels_received_count == NUM_CHANNELS) {

            for (int ch = 0; ch < NUM_CHANNELS; ++ch) {
                const auto& y = cal_mv_channels[ch];
                std::ostringstream oss;
                oss << "{\"type\":\"waveform\",\"x\":[";
                for (size_t i = 0; i < y.size(); ++i) {
                    oss << i;
                    if (i + 1 < y.size()) oss << ',';
                }
                oss << "],\"y\":[";
                oss << std::fixed << std::setprecision(3);
                for (size_t i = 0; i < y.size(); ++i) {
                    oss << y[i];
                    if (i + 1 < y.size()) oss << ',';
                }
                oss << "],\"unit\":\"mV\"}";

                std::string payload = oss.str();
                std::string key = "slowdash:wave:ch" + std::to_string(ch);

                // redisReply* reply = (redisReply*)redisCommand(redisCtx, "SET %s %s",
                //                                               key.c_str(), payload.c_str());
                // if (reply) freeReplyObject(reply);
                gDB->write(key, payload);
            }
            std::cout << "Published " << NUM_CHANNELS << " channels to Redis\n";
            // Reset for next event
            for (int ch = 0; ch < NUM_CHANNELS; ++ch)
                channelReceived[ch] = false;

            channels_received_count = 0;
        }

    }


    return true;
}

//_____________________________________________________________________________
void RoestiDash::Init()
{
    PrintConfig(fConfig, "channel-config", __PRETTY_FUNCTION__);
    PrintConfig(fConfig, "chans.", __PRETTY_FUNCTION__);

    fNumMessages = 0;
    fopened = false;

    if (gDB) {
        gDB = nullptr;
    }
    
}

//_____________________________________________________________________________
void RoestiDash::InitTask()
{
    PrintConfig(fConfig, "channel-config", __PRETTY_FUNCTION__);
    PrintConfig(fConfig, "chans.", __PRETTY_FUNCTION__);

    // LOG(debug) << MyClass << " InitTask";
    LOG(debug) <<  " InitTask";
    using opt = OptionKey;

    fInputChannelName = fConfig->GetProperty<std::string>(opt::InputChannelName.data());
    LOG(debug) << " input channel = " << fInputChannelName;


    const auto &isMultipart = fConfig->GetProperty<std::string>(opt::Multipart.data());
    if (isMultipart=="true" || isMultipart=="1") {
        LOG(warn) << " set multipart data handler";
        OnData(fInputChannelName, &RoestiDash::HandleMultipartData);
    } else {
        LOG(warn) << " set data handler";
        OnData(fInputChannelName, &RoestiDash::HandleData);
    }

    // LoadPedestalForBoard("/home/frost/dev/comet/userworks-change/roesti/baseline_9E.txt");

}

//_____________________________________________________________________________
void RoestiDash::PreRun()
{
    const auto rnumber = fConfig->GetProperty<std::string>("run_number");
    LOG(debug) << "run_number: " << rnumber;

}
//_____________________________________________________________________________
void RoestiDash::PostRun()
{

    fTFBuffer.clear();

    using opt = OptionKey;
    LOG(debug) << __func__;
    int nrecv=0;
    while (true) {
        const auto &isMultipart = fConfig->GetProperty<std::string>(opt::Multipart.data());
        if (isMultipart=="true" || isMultipart=="1") {
            FairMQParts parts;
            if (Receive(parts, fInputChannelName) <= 0) {
                LOG(debug) << __func__ << " no data received " << nrecv;
                ++nrecv;
                if (nrecv>5) {
                    LOG(debug) << __func__ << " done";
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(200));
            } else {
                // LOG(debug) << __func__ << " print data";
                HandleMultipartData(parts, 0);
            }
        }
        LOG(debug) << __func__ << " still receiving";
    }

    if( fstr.is_open() ) {
        fstr.flush();
        fstr.close();
    }

    if (gDB) {
        delete gDB;
        gDB = nullptr;
    }

}
