//________________________________________________________________________________________________Recbe example
#include <chrono>
#include <ios>
#include <ostream>
#include <thread>
#include <vector>
#include <numeric>
#include <iostream>
#include <iomanip>
#include <functional>
#include <thread>
#include <cassert>
#include <algorithm>
#include <string>


#include "TBufferFile.h"
#include "TFile.h"
#include "TTree.h"
#include "TLeaf.h" 
#include "TObject.h"

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
bool RoestiSink::HandleMultipartData(FairMQParts &msgParts, int index)
{

    // std::string output_file = argv[3];
    // char filename[256];
    // sprintf(filename, "./data/run%06d.dat", stoi(rnumber));
    // std::ofstream outfile(filename, std::ios::binary);
    // outfile.write(buffer, bytes_received);
    // outfile.close();

    // for (const auto& msg : msgParts) {
    //     const auto ptr = reinterpret_cast<char*>(msg->GetData());
    //     std::string s(ptr, ptr+msg->GetSize());
    //     LOG(debug) << __FUNCTION__ << " received = " << s << " [" << index << "] " << fNumMessages;
    //     LOG(debug) << s;
    //     fstr.write(ptr,msg->GetSize());
    //     ++fNumMessages;
    // }


    // ---raw buffer variant
    // auto length = TotalLength(msgParts);
    // std::cout << "TotalLength:" <<length << std::endl;
    std::vector<char> rawbuf;
    int full_size = msgParts.At(0)->GetSize() + msgParts.At(1)->GetSize();
    rawbuf.reserve(full_size);
    // std::cout << "Total len for file: "<< length << std::endl;
    // FairMQParts inParts;

    // TFile rfl("tree.root","recreate");
    // rfl->WriteObject(hist, "MyHist");
    
    TFile* rtfile = new TFile("buffer_output.root", "RECREATE");
    if (rtfile->IsZombie()) {
        std::cerr << "Error creating ROOT file!" << std::endl;
        return 1;
    }
    
    TTree* tree = new TTree("chunkTree", "Tree with chunked buffer data");

    // Create a TTree to hold the buffer data
    // TTree* tree = new TTree("bufferTree", "Tree storing buffer data");
    // float value;  // Temporary variable for tree branch
    // tree->Branch("value", &value, "value/F");


    // --why recieve fires second time???
    //  if (Receive(msgParts, fInputChannelName, 0, 1) > 0) {
        // assert(msgParts.Size() >= 2);
        
        // LOG(warn) << "----------------msgSize----->" << msgParts.Size();
        // auto packetSize = msgParts.At(1)->GetSize();

        LOG(debug4) << " received message parts size [0] = " << msgParts.At(0)->GetSize();
        // LOG(debug4) << "msg size: " << msgParts.Size() << std::endl;
        LOG(debug4) << "received message parts size [1] = " << msgParts.At(1)->GetSize();

        
        auto stfHeader = reinterpret_cast<SubTimeFrame::Header*>(msgParts.At(0)->GetData());
        auto stfId     = stfHeader->timeFrameId;
        LOG(warn) << "-----STF HEADER-----" << stfHeader;
        LOG(debug4) << "DataPacket length:" << stfHeader->length;
        LOG(debug4) << "event number: "<< stfId;
        LOG(debug4) << "Board IP: "<< stfHeader->FEMId;


        char *recieveBuf = reinterpret_cast<char* >(msgParts.At(1)->GetData());

        // hexdump(recieveBuf, msgParts.At(1)->GetSize());

        struct Roesti::RoestiHeader *rhead = reinterpret_cast<struct Roesti::RoestiHeader *>(recieveBuf);
        LOG(warn) << "-----Roesti Header-----";
        LOG(debug4) << "Roesti Header magic--->" << rhead->magic;
        LOG(debug4) << "Roesti Header length--->" << rhead->length;
        LOG(debug4) << "Roesti Header event--->" << rhead->event_num;
        LOG(debug4) << "Roesti Header ip--->" << static_cast<uint32_t>(rhead->ipaddr & 0xff);
        LOG(debug4) << "Roesti Header seu--->" << rhead->seu_count;
        LOG(debug4) << "Roesti Header mbu--->" << rhead->mbu_count;
        LOG(debug4) << "Roesti Header seumb--->" << rhead->seu_inf_size;

        // Create ROOT file from an existing buffer
        // TBufferFile *bufFromBuffer = new TBufferFile(TBuffer::kWrite, full_size, recieveBuf);

        // struct Roesti::RoestiDataBody *rdpack = reinterpret_cast<Roesti::RoestiDataBody*>(recieveBuf+sizeof(Roesti::RoestiHeader));
        // LOG(warn) << "-----Roesti Data-----";
        // LOG(debug4) << "RoestiWord->" << rdpack->data_packet_word;
        // LOG(debug4) << "RoestiCH#->" << rdpack->channel_num;
        // LOG(debug4) << "RoestiStop#->" << rdpack->channel_num;
        // LOG(debug4) << "RoestiDatalength->" << rdpack->data_length;
        // LOG(debug4) << "RoestiADC->" << rdpack->data;
        // LOG(debug4) << "RoestiFooter->" << rdpack->data_footer;
        // LOG(debug4) << "Roestikeyword->" << rdpack->keyword<<std::endl;



        // Create a TBufferFile (used to simulate writing with TFileBuffer-like logic)
        // TBufferFile rtbuff(TBuffer::kWrite);

        // // Write buffer size and data manually to the TBuffer
        // int buffer_size = msgParts.At(1)->GetSize();
        // rtbuff.WriteInt(buffer_size);
        // rtbuff.WriteObject(msgParts.At(1)->GetData(), buffer_size);

        // // Now write this buffer as a binary blob into the ROOT file
        // rtfile->WriteObject(&rtbuff, "RawBuffer");

        // rtfile->Close();


        //-define buffer boundries
        unsigned int packet_read = 0;
        unsigned int packets_full_length = rhead->length;
        char *datapackets = reinterpret_cast<char* >(recieveBuf+sizeof(struct Roesti::RoestiHeader));

        //-vector for saving data
        // std::vector<char> data_packs_content ;
        // data_packs_content.clear();
        // data_packs_content.reserve(packets_full_length);
        int ch_iter = 0;
       
        const int CHUNK_SIZE = 2048;
        const int NUM_BRANCHES = 18;
        char* chunks[NUM_BRANCHES];
        chunks[ch_iter] = new char[CHUNK_SIZE];
        std::cout<< "--------------------DATA--------------------" << std::endl;
        while(packet_read < packets_full_length){
            std::cout << "data packet read/length: " << packet_read << "/" << packets_full_length << std::endl;
            
            struct Roesti::DataPacketHeader *rdpackhead = reinterpret_cast<Roesti::DataPacketHeader *>(datapackets);
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

            struct Roesti::DataPacketData *rdpackdata = reinterpret_cast<Roesti::DataPacketData*>(datapackets
                                                                                              +sizeof(struct Roesti::DataPacketHeader));
            // LOG(debug4) << "DATA->" << rdpackdata->data;
            // LOG(debug4) << "----DataBufferSize" << sizeof(rdpackdata->data);

            struct Roesti::DataPacketFooter *rdpackfooter = reinterpret_cast<Roesti::DataPacketFooter *>(datapackets
                                                                                                    +sizeof(struct Roesti::DataPacketHeader)
                                                                                                    +rdpackhead->data_length );
            // LOG(debug4) << "DataPackFooter Word->" << rdpackfooter->footer;
            std::cout << "DP footer: 0x" << std::hex << rdpackfooter->footer;
		    std::cout << ", keyword: 0x" << static_cast<unsigned int>(rdpackfooter->keyword);
		    std::cout << ", res: 0x" << static_cast<unsigned int>(rdpackfooter->reserved);
		    std::cout << std::dec << std::endl;

            // data_packs_content.insert(data_packs_content.begin()+ch_iter, datapackets, sizeof(struct Roesti::DataPacketHeader)+rdpackhead->data_length + sizeof(struct Roesti::DataPacketFooter));
            

            packet_read += sizeof(struct Roesti::DataPacketHeader)+rdpackhead->data_length + sizeof(struct Roesti::DataPacketFooter);
            
            // data_packs_content.insert(data_packs_content.end(), *datapackets, sizeof(packet_read));

            // const auto ptr = datapackets;
            // const auto ptr_next = ptr + packet_read;
            // data_packs_content.insert(data_packs_content.end(),
            //                           std::make_move_iterator(reinterpret_cast<char *>(ptr)),
            //                           std::make_move_iterator(reinterpret_cast<char *>(ptr_next)) );

            datapackets += sizeof(struct Roesti::DataPacketHeader)+rdpackhead->data_length + sizeof(struct Roesti::DataPacketFooter);
            
            

        
            std::string branchName = "chunk" + std::to_string(ch_iter);
            tree->Branch(branchName.c_str(), chunks[ch_iter], (branchName + "[" + std::to_string(CHUNK_SIZE) + "]/B").c_str());

            // std::memcpy(chunks[i], &datapackets, packet_read);
            std::memcpy(chunks[ch_iter] , &datapackets, 2048);

            //Fill the tree
            tree->Fill();
            ch_iter++;
        }

        tree->Write();
        rtfile->Close();

        //-cast to full packet struct, data size wrong
        // struct Roesti::RoestiDataPacket *rfullpack = reinterpret_cast<Roesti::RoestiDataPacket* >(recieveBuf+sizeof(struct Roesti::RoestiHeader));
        // LOG(debug4) << "DataPackHeader Word->" << rfullpack->data_packet_word;
        // LOG(debug4) << "DataPackHeader CH#->" << static_cast<unsigned int>(rfullpack->channel_num);
        // LOG(debug4) << "DataPackHeader Stop#->" << rfullpack->stop_num;
        // LOG(debug4) << "DataPackFooter Word->" << rfullpack->data_footer;

        //----check input with file
        // char *dbody = reinterpret_cast<char* >(recieveBuf);
        // dbody += sizeof(recieveBuf) + sizeof(struct Roesti::RoestiHeader)+sizeof(struct Roesti::DataPacketHeader);
        // std::ofstream outputFile("./dataout/input_body.bin", std::ios::binary);
        // outputFile.write(dbody,rdpackhead->data_length);
        // outputFile.close();

        
        // vectorBuffer.reserve(TotalLength(msgParts.At(1)->GetData()));
        // memcpy(pStartBuffer, &rdpack, msgParts.At(1)->GetSize());

        // rawbuf.insert(rawbuf.end(), 
        //                 reinterpret_cast<const char*>(rdpack->data), 
        //                 reinterpret_cast<const char*>(rdpack->data)+sizeof(rdpack->data));

        // if (fTFBuffer.find(stfId) == fTFBuffer.end()) {
        //     fTFBuffer[stfId].reserve(fNumSource);
        // }
        // fTFBuffer[stfId].emplace_back(STFBuffer {std::move(msgParts), std::chrono::steady_clock::now()});

        // if (!fTFBuffer.empty()) {
        //     LOG(info) << fTFBuffer[stfId];
        //     fTFBuffer.clear();
        // }

        // ---raw buffer variant
        // const auto ptr = rdpackdata->data;
        // const auto ptr_next = ptr + msgParts.At(1)->GetSize();//sizeof(rdpack->data); //msgParts.Size();
        // // // // LOG(debug4) << "--raw-->" << ptr << std::endl;
        // LOG(debug4) << "part: "<< ptr;
        // LOG(debug4) << "part length: "<< TotalLength(ptr);
        // LOG(debug4) << "part next: "<< ptr_next;
        // LOG(debug4) << "part next length: "<< TotalLength(ptr_next);
        // rawbuf.insert(
        //     rawbuf.end(),
        //     std::make_move_iterator(reinterpret_cast<char *>(ptr)),
        //     std::make_move_iterator(reinterpret_cast<char *>(ptr_next)));

        if( fstr.is_open() && fopened ) {
            // fstr.write(rawbuf.data(), length);
            fstr.write(reinterpret_cast<char* >(recieveBuf), msgParts.At(1)->GetSize());
        }

        // ---memcp to buffer variant
        // char* pStartBuffer ; //= reinterpret_cast<char *>(rdpackdata);
        // memcpy(pStartBuffer, &rdpackdata, rhead->length);

        // if( fstr.is_open() && fopened ) {
        //     // fstr.write(reinterpret_cast<const char*>(rdpack->data), rdpack->data_length);
        //     fstr.write(pStartBuffer, rdpackhead->data_length);
        // }

    // }


    return true;
}

// bool RoestiSink::Save2File(){
//     return false;
// }

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
