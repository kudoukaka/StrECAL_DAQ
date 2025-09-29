//________________________________________________________________________________________________Recbe example
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

#include <fairmq/runFairMQDevice.h>
// #include <fairmq/runDevice.h>
// #include <fairmq/Device.h>

#include "RoestiSink.h"
#include "SubTimeFrameHeader.h"
#include "roesti.h"


#include "hexdump.cxx"
#include "utility/Hexdump.hpp"

// static constexpr std::string_view MyClass{"RoestiSink"};

namespace bpo = boost::program_options;

//_____________________________________________________________________________
void addCustomOptions(bpo::options_description &options)
{
    using opt = RoestiSink::OptionKey;
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
    return new RoestiSink;
}

// FairMQDevicePtr getDevice(const FairMQProgOptions &)
// {
//     return new RoestiSink;
// }

// std::unique_ptr<fair::mq::Device> getDevice(FairMQProgOptions&)
// {
//     return std::make_unique<RoestiSink>();
// }

//______________________________________________________________________________
// /}


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
bool RoestiSink::HandleData(FairMQMessagePtr &msg, int index)
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
uint64_t RoestiSink::TotalLength(const FairMQParts& parts)
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
const int NUM_CAPACITORS = 1024;

int pedestal[NUM_CHANNELS][NUM_CAPACITORS] = {{0}};

bool LoadPedestalForBoard(const std::string& filename)
{

    std::ifstream infile(filename);
    if (!infile.is_open()) {
        std::cerr << "Failed to open pedestal CSV file: " << filename << std::endl;
        return false;
    }

    std::string line;
    int line_num = 0;

    while (std::getline(infile, line)) {
        ++line_num;
        if (line.empty()) continue;

        std::stringstream ss(line);
        std::string token;

        int channel = -1;
        int capacitor = -1;
        int value = -1;

        std::getline(ss, token, ',');
        channel = std::stoi(token);

        std::getline(ss, token, ',');
        capacitor = std::stoi(token);

        std::getline(ss, token, ',');
        value = std::stoi(token);

        if (channel < 0 || channel >= NUM_CHANNELS ||
            capacitor < 0 || capacitor >= NUM_CAPACITORS) {
            std::cerr << "Invalid entry at line " << line_num << ": " << line << std::endl;
            continue;
        }

        pedestal[channel][capacitor] = value;
    }

    infile.close();
    std::cout << "Pedestal values loaded from: " << filename << std::endl;
    return true;
}

//_____________________________________________________________________________
bool RoestiSink::HandleMultipartData(FairMQParts &msgParts, int index)
{

    auto stfHeader = reinterpret_cast<SubTimeFrame::Header*>(msgParts.At(0)->GetData());
    auto stfId     = stfHeader->timeFrameId;

    char *receiveBuf = reinterpret_cast<char* >(msgParts.At(1)->GetData());

    #ifdef DEBUG
        LOG(debug4) << " received message parts size [0] = " << msgParts.At(0)->GetSize();
        LOG(debug4) << "received message parts size [1] = " << msgParts.At(1)->GetSize();

        LOG(debug4) << "-----STF HEADER-----" << stfHeader;
        LOG(debug4) << "DataPacket length:" << stfHeader->length;
        LOG(debug4) << "event number: "<< stfId;
        LOG(debug4) << "Board IP: "<< stfHeader->FEMId;
    #endif

    // struct Roesti::RoestiHeader *rhead = reinterpret_cast<struct Roesti::RoestiHeader *>(receiveBuf);
    Roesti::RoestiHeader* rhead = reinterpret_cast<Roesti::RoestiHeader*>(receiveBuf);
    unsigned int packets_full_length = rhead->length;

    char* datapackets = receiveBuf + sizeof(Roesti::RoestiHeader);
    unsigned int packet_read = 0;
    int packet_index = 0;

    //-define buffer boundries
    // unsigned int packet_read = 0;
    // unsigned int packets_full_length = rhead->length;
    // char *datapackets = reinterpret_cast<char* >(receiveBuf+sizeof(struct Roesti::RoestiHeader));

    // int ch_iter = 0;
    // const int CHUNK_SIZE = 2048;
    // const int NUM_BRANCHES = 17;
    // char* chunks[NUM_BRANCHES];


    #ifdef DEBUG
        LOG(debug4) << "-----Roesti Header-----";
        LOG(debug4) << "Roesti Header magic--->" << rhead->magic;
        LOG(debug4) << "Roesti Header length--->" << rhead->length;
        LOG(debug4) << "Roesti Header event--->" << rhead->event_num;
        LOG(debug4) << "Roesti Header ip--->" << static_cast<uint32_t>(rhead->ipaddr & 0xff);
        LOG(debug4) << "Roesti Header seu--->" << rhead->seu_count;
        LOG(debug4) << "Roesti Header mbu--->" << rhead->mbu_count;
        LOG(debug4) << "Roesti Header seumb--->" << rhead->seu_inf_size;
    #endif

   
    // --- Create unique ROOT file ---
    std::ostringstream filename;
    filename << "CalibratedWaveforms_STF_N" << stfId << ".root";
    TFile* rtfile = new TFile(filename.str().c_str(), "RECREATE");
    if (rtfile->IsZombie()) {
        std::cerr << "Error creating ROOT file: "<< filename.str() << std::endl;
        return false;
    }
    
    const int MAX_SAMPLES = 1024;

    TTree* dataTree = new TTree("waveformTree", "ADC Calibrated Waveforms");
    // dataTree->Branch("stfId", &stfId);
    TGraph* channelGraphs[NUM_CHANNELS] = {nullptr};

    // Create TGraph branches
    for (int ch = 0; ch < NUM_CHANNELS; ++ch) {
        std::string branchName = "ch" + std::to_string(ch);
        dataTree->Branch(branchName.c_str(), &channelGraphs[ch]);
    }

    // Optional: add event or STF ID as metadata
    uint32_t event_id = rhead->event_num;
    dataTree->Branch("event_id", &event_id, "event_id/i");


    bool channelReceived[NUM_CHANNELS] = {false};
    int channels_received_count = 0;

    // Vectors to hold ADC data for each channel
    // std::vector<std::vector<double>> adcData(NUM_CHANNELS);
    // std::vector<std::vector<double>> adcTime(NUM_CHANNELS); // time sample index

    #ifdef DEBUG
        std::cout<< "--------------------DATA--------------------" << std::endl;
    #endif

    while(packet_read < packets_full_length){

        /* 
        //----------manual packet size definitions
        struct Roesti::DataPacketHeader *rdpackhead = reinterpret_cast<Roesti::DataPacketHeader *>(datapackets);

        #ifdef DEBUG
            std::cout << "data packet read/length: " << packet_read << "/" << packets_full_length << std::endl;
            // LOG(debug4) << "DataPackHeader Word->" << rdpackhead->data_packet_word;
            // LOG(debug4) << "DataPackHeader CH#->" << static_cast<unsigned int>(rdpackhead->channel_num);
            // LOG(debug4) << "DataPackHeader Stop#->" << rdpackhead->stop_num;
            // LOG(debug4) << "DataPackHeader DataLength->" << rdpackhead->data_length;
            std::cout << "DP MAGIC: 0x" << std::hex << rdpackhead->data_packet_word;
		    std::cout << ", res: 0x" << std::hex << static_cast<unsigned int>(rdpackhead->reserved_header);
		    std::cout << ", ch: " << std::dec << static_cast<unsigned int>(rdpackhead->channel_num);
		    std::cout << ", stop: " << std::dec << rdpackhead->stop_num;
		    std::cout << ", Len: " << std::dec << rdpackhead->data_length;
		    std::cout << std::endl;
        #endif

        struct Roesti::DataPacketData *rdpackdata = reinterpret_cast<Roesti::DataPacketData*>(datapackets
                                                                                              +sizeof(struct Roesti::DataPacketHeader));
        // LOG(debug4) << "DATA->" << rdpackdata->data;
        // LOG(debug4) << "----DataBufferSize" << sizeof(rdpackdata->data);

        struct Roesti::DataPacketFooter *rdpackfooter = reinterpret_cast<Roesti::DataPacketFooter *>(datapackets
                                                                                                    +sizeof(struct Roesti::DataPacketHeader)   
        
        #ifdef DEBUG                                                                                            +rdpackhead->data_length );
            // LOG(debug4) << "DataPackFooter Word->" << rdpackfooter->footer;
            std::cout << "DP footer: 0x" << std::hex << rdpackfooter->footer;
		    std::cout << ", keyword: 0x" << static_cast<unsigned int>(rdpackfooter->keyword);
		    std::cout << ", res: 0x" << static_cast<unsigned int>(rdpackfooter->reserved);
		    std::cout << std::dec << std::endl;
        #endif
        // data_packs_content.insert(data_packs_content.begin()+ch_iter, datapackets, sizeof(struct Roesti::DataPacketHeader)+rdpackhead->data_length + sizeof(struct Roesti::DataPacketFooter));
            
        packet_read += sizeof(struct Roesti::DataPacketHeader)+rdpackhead->data_length + sizeof(struct Roesti::DataPacketFooter);
            
        // data_packs_content.insert(data_packs_content.end(), *datapackets, sizeof(packet_read));

        // const auto ptr = datapackets;
        // const auto ptr_next = ptr + packet_read;
        // data_packs_content.insert(data_packs_content.end(),
        //                           std::make_move_iterator(reinterpret_cast<char *>(ptr)),
        //                           std::make_move_iterator(reinterpret_cast<char *>(ptr_next)) );

        datapackets += sizeof(struct Roesti::DataPacketHeader)+rdpackhead->data_length + sizeof(struct Roesti::DataPacketFooter);
        */

        //----------auto packet size definition
        auto* rdpackhead = reinterpret_cast<Roesti::DataPacketHeader*>(datapackets);

        uint8_t channel = rdpackhead->channel_num;
        uint16_t dataLength = rdpackhead->data_length;
        uint16_t stop_num = rdpackhead->stop_num;

        if (channel >= NUM_CHANNELS) {
            std::cerr << "Invalid channel number: " << static_cast<int>(channel) << std::endl;
            break;
        }

        size_t num_samples = dataLength / 2;
        if (num_samples > MAX_SAMPLES) num_samples = MAX_SAMPLES;

        std::vector<double> adc_data(num_samples);
        std::vector<double> sample_idx(num_samples);

        //--packet data
        auto* data = reinterpret_cast<uint8_t*>(datapackets + sizeof(Roesti::DataPacketHeader));

        
        //Clean up old graph if any
        if (channelGraphs[channel]) {
            delete channelGraphs[channel];
            channelGraphs[channel] = nullptr;
        }

         // Create new TGraph with calibrated data
        channelGraphs[channel] = new TGraph(num_samples);

        for (size_t i = 0; i < num_samples; ++i) {
            uint16_t raw_adc = *(reinterpret_cast<uint16_t*>(data + i * 2)) & 0x0FFF;
            int capacitor_number = (i + stop_num) % 1024;

            int corrected = raw_adc;

            // Apply pedestal correction only for ADC channels (0–15)
            if (channel < 16) {
                corrected -= pedestal[channel][capacitor_number];
            }

            //1
            // float mv = (float)corrected / 2048.0f * 1000.0f;
            // adc_data[i] = mv;
            // sample_idx[i] = i;

            double mv = static_cast<double>(corrected) / 2048.0 * 1000.0;
            channelGraphs[channel]->SetPoint(i, i, mv);
        }

        channelGraphs[channel]->SetTitle(Form("Channel %d", channel));
       
        //1
        // if (channelGraphs[channel]) {
        //     delete channelGraphs[channel];
        //     channelGraphs[channel] = nullptr;
        // }
        // Mark channel received if not already

        //--count received channels
        if (!channelReceived[channel]) {
            channelReceived[channel] = true;
            channels_received_count++;
        }

        //1
        // channelGraphs[channel] = new TGraph(num_samples, sample_idx.data(), adc_data.data());
        // channelGraphs[channel]->SetTitle(Form("Channel %d", channel));



        //--Advance to next packet
        packet_read += sizeof(Roesti::DataPacketHeader) + dataLength + sizeof(Roesti::DataPacketFooter);
        datapackets += sizeof(Roesti::DataPacketHeader) + dataLength + sizeof(Roesti::DataPacketFooter);


        

        // Check if all channels have arrived
        if (channels_received_count == NUM_CHANNELS) {
            dataTree->Fill();

            // Optional: Plot all channels together
            // TCanvas* c1 = new TCanvas("c1", "Calibrated ADC Waveforms", 1200, 800);
            // c1->Divide(3, 6); // 18 pads for 18 channels

            // for (int ch = 0; ch < NUM_CHANNELS; ++ch) {
            //     c1->cd(ch + 1);
            //     channelGraphs[ch]->SetLineColor(kBlue);
            //     channelGraphs[ch]->SetMarkerStyle(20);
            //     channelGraphs[ch]->Draw("AL"); // Axis + Line
            // }

            // c1->Update();
            // c1->SaveAs(Form("Waveforms_Event_%u.png", event_id));
            // delete c1;

            // Cleanup graphs for next event
            for (int ch = 0; ch < NUM_CHANNELS; ++ch) {
                delete channelGraphs[ch];
                channelGraphs[ch] = nullptr;
                channelReceived[ch] = false;
            }
            channels_received_count = 0;
        }

    }
    // --- Create TGraphs and write to them ---
    rtfile->cd();
    // 1
    // for (int ch = 0; ch < NUM_CHANNELS; ++ch) {
    //     std::ostringstream gname;
    //     gname << "channel_" << ch;

    //     TGraph* graph = new TGraph(adcData[ch].size(),
    //                                adcTime[ch].data(),
    //                                adcData[ch].data());

    //     graph->SetTitle(gname.str().c_str());
    //     graph->GetXaxis()->SetTitle("Sample");
    //     graph->GetYaxis()->SetTitle("ADC Value");
    //     graph->Write(gname.str().c_str());
    // }

    // // Write tree
    // dataTree->Fill();
    dataTree->Write();
    rtfile->Close();
    delete rtfile;

    // Safety cleanup (if last set incomplete)
    for (int i = 0; i < NUM_CHANNELS; ++i) {
        if (channelGraphs[i]) {
            delete channelGraphs[i];
            channelGraphs[i] = nullptr;
        }
    }

    return true;
}

//_____________________________________________________________________________
void RoestiSink::Init()
{
    PrintConfig(fConfig, "channel-config", __PRETTY_FUNCTION__);
    PrintConfig(fConfig, "chans.", __PRETTY_FUNCTION__);

    fNumMessages = 0;
    fopened = false;
}

//_____________________________________________________________________________
void RoestiSink::InitTask()
{
    PrintConfig(fConfig, "channel-config", __PRETTY_FUNCTION__);
    PrintConfig(fConfig, "chans.", __PRETTY_FUNCTION__);

    // LOG(debug) << MyClass << " InitTask";
    LOG(debug) <<  " InitTask";
    using opt = OptionKey;

    fInputChannelName = fConfig->GetProperty<std::string>(opt::InputChannelName.data());
    LOG(debug) << " input channel = " << fInputChannelName;

    //////
    /*  FairMQMessagePtr msginfo(NewMessage());
    int nrecv=0;
    while(true){
      if (Receive(msginfo, fInputChannelName) <= 0) {
        LOG(debug) << __func__ << " Trying to get FEMIfo " << nrecv;
        nrecv++;
      }else{
        break;
      }
    }

    const auto ptr = reinterpret_cast<unsigned char*>(msginfo->GetData());

    for(size_t i = 0; i < msginfo->GetSize(); i++){
      printf("%02x", ptr[i]);
      if( ( (i+1)%8 )==0 )
        printf("\n");
    }
    */
    ////

    const auto &isMultipart = fConfig->GetProperty<std::string>(opt::Multipart.data());
    if (isMultipart=="true" || isMultipart=="1") {
        LOG(warn) << " set multipart data handler";
        OnData(fInputChannelName, &RoestiSink::HandleMultipartData);
    } else {
        LOG(warn) << " set data handler";
        OnData(fInputChannelName, &RoestiSink::HandleData);
    }

    LoadPedestalForBoard("/home/frost/dev/comet/userworks-change/roesti/baseline_9E.txt");

}

//_____________________________________________________________________________
void RoestiSink::PreRun()
{
    const auto rnumber = fConfig->GetProperty<std::string>("run_number");
    LOG(debug) << "run_number: " << rnumber;

    //  std::string filename = "run" + rnumber + ".dat";
    //  fstr.open(filename.data(), std::ios::binary);

    char filename[256];
    sprintf(filename, "./dataout/run%06d.dat", stoi(rnumber));
    fstr.open(filename, std::ios::binary);
    fopened = true;

}
//_____________________________________________________________________________
void RoestiSink::PostRun()
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
                if (nrecv>10) {
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(200));
            } else {
                LOG(debug) << __func__ << " print data";
                HandleMultipartData(parts, 0);
            }
        }
        //  else {
        //     FairMQMessagePtr msg(NewMessage());
        //     if (Receive(msg, fInputChannelName) <= 0) {
        //         LOG(debug) << __func__ << " no data received " << nrecv;
        //         ++nrecv;
        //         if (nrecv>10) {
        //             break;
        //         }
        //         std::this_thread::sleep_for(std::chrono::milliseconds(200));
        //     } else {
        //         LOG(debug) << __func__ << " print data";
        //         HandleData(msg, 0);
        //     }
        // }
        LOG(debug) << __func__ << " done";
    }

    if( fstr.is_open() ) {
        fstr.flush();
        fstr.close();
    }

}
