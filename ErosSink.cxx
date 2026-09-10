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
#include <map>
#include <ctime>
#include <filesystem>
#include <cstring> // for memcpy() used when reading ADC samples


#include "TFile.h"
#include "TTree.h"
#include "TLeaf.h"
#include "TObject.h"
#include "TAxis.h" 
#include <TGraph.h>
#include <TCanvas.h>
#include <arpa/inet.h> // For ntohs()

#include <fairmq/runFairMQDevice.h>


#include "ErosSink.h"
#include "SubTimeFrameHeader.h"
#include "eros.h"


#include "hexdump.cxx"
#include "utility/Hexdump.hpp"


namespace bpo = boost::program_options;

//_____________________________________________________________________________
void addCustomOptions(bpo::options_description &options)
{
    using opt = ErosSink::OptionKey;
    options.add_options()
    (opt::Multipart.data(), bpo::value<std::string>()->default_value("true"), "Handle multipart message\n")
    (opt::InputChannelName.data(),     bpo::value<std::string>()->default_value("in"),        "Name of the input channel")
    ("generate-event-id",              bpo::value<std::string>()->default_value("false"),
        "Ignore event_id sent by the device and generate it from an internal per-message counter instead (useful when devices always send event_id=0)\n");
}



//_____________________________________________________________________________

FairMQDevicePtr getDevice(const FairMQProgOptions &)
{
    return new ErosSink;
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
bool ErosSink::HandleData(FairMQMessagePtr &msg, int index)
{

    LOG(info) << "Received: \""
			<< std::string(static_cast<char*>(msg->GetData()), msg->GetSize())
			<< "\"";

    return true;
}

//_____________________________________________________________________________
uint64_t ErosSink::TotalLength(const FairMQParts& parts)
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

// ---------- event_id generation (Req. 2) ----------
static bool     gUseEventIdCounter = false;   // set from "generate-event-id" option in InitTask()
static uint32_t gEventIdCounter    = 0;       // incremented on every received message when enabled

static TFile*   gRtFile = nullptr;
static TTree*   gTree   = nullptr;
static TGraph*  gRawGraphs[NUM_CHANNELS] = {nullptr};
static uint32_t gTreeEventId = 0;
static uint32_t gTreeIpAddr  = 0;
static UShort_t gStopNums[NUM_CHANNELS] = {0}; // this event's real stop_num, one per channel
static std::string gRunStartTimestamp; // set once in PreRun(), used to build the run's filename

//_____________________________________________________________________________

void OpenRunFile()
{
    if (gRtFile) {
        return; // already open for this run
    }

    std::filesystem::create_directories("storage");

    std::ostringstream filename;
    filename << "output_" << gRunStartTimestamp << ".root";
    gRtFile = new TFile(filename.str().c_str(), "RECREATE");
    if (gRtFile->IsZombie()) {
        std::cerr << "Error creating ROOT file: " << filename.str() << std::endl;
        return;
    }

    for (int ch = 0; ch < NUM_CHANNELS; ++ch) {
        gRawGraphs[ch] = new TGraph();
    }

    gTree = new TTree("waveformTree", "Raw ADC Waveforms");
    for (int ch = 0; ch < NUM_CHANNELS; ++ch) {
        std::string rawBranch = "raw_ch" + std::to_string(ch);
        gTree->Branch(rawBranch.c_str(), "TGraph", &gRawGraphs[ch]);
    }

    gTree->Branch("event_id", &gTreeEventId, "event_id/i");
    gTree->Branch("ipaddr",   &gTreeIpAddr,  "ipaddr/i");

    gTree->Branch("stop_num", gStopNums, Form("stop_num[%d]/s", NUM_CHANNELS));
}

//_____________________________________________________________________________

bool ErosSink::HandleMultipartData(FairMQParts &msgParts, int index)
{

    auto stfHeader = reinterpret_cast<SubTimeFrame::Header*>(msgParts.At(0)->GetData());
    auto stfId     = stfHeader->timeFrameId;

    char *receiveBuf = reinterpret_cast<char* >(msgParts.At(1)->GetData());



    Eros::ErosHeader* rhead = reinterpret_cast<Eros::ErosHeader*>(receiveBuf);
    unsigned int packets_full_length = rhead->length;

    char* datapackets = receiveBuf + sizeof(Eros::ErosHeader);
    unsigned int packet_read = 0;
    int packet_index = 0;

    uint32_t event_id = gUseEventIdCounter ? gEventIdCounter++ : rhead->event_num;

    uint32_t ipaddr   = static_cast<uint32_t>(rhead->ipaddr & 0xff);


    LOG(debug) << "RecievedEvent# " << event_id << "- From board: " << static_cast<uint32_t>(rhead->ipaddr & 0xff);


    OpenRunFile();
    gRtFile->cd();
    gTreeEventId = event_id;
    gTreeIpAddr  = ipaddr;

    bool channelReceived[NUM_CHANNELS] = {false};
    int channels_received_count = 0;

    // Vectors to hold ADC data for each channel
    std::vector<std::vector<double>> adcData(NUM_CHANNELS);
    std::vector<std::vector<double>> adcTime(NUM_CHANNELS); 

    #ifdef DEBUG
        std::cout<< "--------------------DATA--------------------" << std::endl;
    #endif

    while(packet_read < packets_full_length){


        //Auto packet size definition
        auto* rdpackhead = reinterpret_cast<Eros::DataPacketHeader*>(datapackets);
        auto* rdpackdata = reinterpret_cast<uint8_t*>(datapackets + sizeof(Eros::DataPacketHeader));
        auto* rdpackfooter = reinterpret_cast<Eros::DataPacketFooter*>(datapackets + sizeof(Eros::DataPacketHeader) + rdpackhead->data_length);

        uint8_t channel = rdpackhead->channel_num;
        uint16_t dataLength = rdpackhead->data_length;
        uint16_t stop_num = rdpackhead->stop_num;


        #ifdef DEBUG
            std::cout << "Channel:" << static_cast<int>(channel)<< std::endl;
            std::cout << "/ StopnNumber: " << stop_num << std::endl;
        #endif

        if (channel >= NUM_CHANNELS) {
            std::cerr << "Invalid channel number: " << static_cast<int>(channel) << std::endl;
            break;
        }

        gStopNums[channel] = stop_num;

        if (dataLength == 0) {
            size_t empty_packet_size = sizeof(Eros::DataPacketHeader) + dataLength + sizeof(Eros::DataPacketFooter);
            packet_read += empty_packet_size;
            datapackets += empty_packet_size;

            #ifdef DEBUG
                std::cout << "Skipping empty data packet, channel:" << static_cast<int>(channel) << std::endl;
            #endif

            continue;
        }

        size_t num_samples = dataLength / 2;
        if (num_samples > NUM_SAMPLE) num_samples = NUM_SAMPLE;

        gRawGraphs[channel]->Set(0);

        for (int sample = 0; sample < num_samples; sample++) {

            uint16_t raw_word = 0;
            memcpy(&raw_word, rdpackdata + sample * 2, sizeof(raw_word));

            uint16_t raw_adc = raw_word & 0x0FFF;

            float raw_mv = static_cast<float>(raw_adc) / 2048.0 * 1000.0;
            gRawGraphs[channel]->SetPoint(static_cast<int>(sample), static_cast<double>(sample), raw_mv);
        }

    
        channelReceived[channel] = true;
        channels_received_count++;

    
        //Advance to next packet
        size_t packet_size = sizeof(Eros::DataPacketHeader) + dataLength + sizeof(Eros::DataPacketFooter);
        packet_read += packet_size;
        datapackets += packet_size;
        

        // Check if all channels have arrived
        if (channels_received_count == NUM_CHANNELS) {
            gTree->Fill();

            gTree->AutoSave("SaveSelf");

            #ifdef DEBUG
                std::cout << "data packet recieved, size/channels:" << packet_size <<"/"<<channels_received_count << std::endl;
            #endif

            // Cleanup graphs for next event
            for (int ch = 0; ch < NUM_CHANNELS; ++ch) {
                channelReceived[ch] = false;
            }
            channels_received_count = 0;
        }

    }


    return true;
}

//_____________________________________________________________________________
void ErosSink::Init()
{
    PrintConfig(fConfig, "channel-config", __PRETTY_FUNCTION__);
    PrintConfig(fConfig, "chans.", __PRETTY_FUNCTION__);

    fNumMessages = 0;
    fopened = false;
}

//_____________________________________________________________________________
void ErosSink::InitTask()
{
    PrintConfig(fConfig, "channel-config", __PRETTY_FUNCTION__);
    PrintConfig(fConfig, "chans.", __PRETTY_FUNCTION__);

    LOG(debug) <<  " InitTask";
    using opt = OptionKey;

    fInputChannelName = fConfig->GetProperty<std::string>(opt::InputChannelName.data());
    LOG(debug) << " input channel = " << fInputChannelName;


    const auto &isMultipart = fConfig->GetProperty<std::string>(opt::Multipart.data());
    if (isMultipart=="true" || isMultipart=="1") {
        LOG(warn) << " set multipart data handler";
        OnData(fInputChannelName, &ErosSink::HandleMultipartData);
    } else {
        LOG(warn) << " set data handler";
        OnData(fInputChannelName, &ErosSink::HandleData);
    }

    const auto &genEvtId = fConfig->GetProperty<std::string>("generate-event-id");
    gUseEventIdCounter = (genEvtId=="true" || genEvtId=="1");
    LOG(debug) << " generate-event-id = " << gUseEventIdCounter;


}

//_____________________________________________________________________________
void ErosSink::PreRun()
{
    const auto rnumber = fConfig->GetProperty<std::string>("run_number");
    LOG(debug) << "run_number: " << rnumber;


    char filename[256];
    sprintf(filename, "./dataout/run%06d.dat", stoi(rnumber));
    fstr.open(filename, std::ios::binary);
    fopened = true;

    {
        std::time_t now = std::time(nullptr);
        std::tm tmNow{};
        localtime_r(&now, &tmNow);
        char tsbuf[32];
        std::strftime(tsbuf, sizeof(tsbuf), "%Y%m%d_%H%M%S", &tmNow);
        gRunStartTimestamp = tsbuf;
        LOG(debug) << "run start timestamp = " << gRunStartTimestamp;
    }
    OpenRunFile();

}
//_____________________________________________________________________________
void ErosSink::PostRun()
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

    if (gTree) {
        gTree->Write("", TObject::kOverwrite);
    }
    if (gRtFile) {
        gRtFile->Close();
        delete gRtFile;
        gRtFile = nullptr;
        gTree = nullptr;
    }

}
