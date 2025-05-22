//________________________________________________________________________________________________Recbe example
#include <chrono>
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

#include <fairmq/runFairMQDevice.h>
// #include <fairmq/runDevice.h>
// #include <fairmq/Device.h>

#include "RoestiSink.h"
#include "SubTimeFrameHeader.h"
#include "roesti.h"


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
    auto length = TotalLength(msgParts);
    std::cout << "TotalLength:" <<length << std::endl;
    std::vector<char> rawbuf;
    rawbuf.reserve(length);
    // std::cout << "Total len for file: "<< length << std::endl;
    // FairMQParts inParts;

     if (Receive(msgParts, fInputChannelName, 0, 1) > 0) {
        // assert(msgParts.Size() >= 2);
        

        LOG(debug4) << " received message parts size 0 = " << msgParts.At(0)->GetSize() << std::endl;
        // LOG(debug4) << "msg size: " << msgParts.Size() << std::endl;

        
        auto stfHeader = reinterpret_cast<SubTimeFrame::Header*>(msgParts.At(0)->GetData());
        auto stfId     = stfHeader->timeFrameId;
        LOG(warn) << "---STF HEADER--->" << stfHeader << std::endl;
        LOG(debug4) << "DP length:" << stfHeader->length << std::endl;
        LOG(debug4) << "stfId(event number): "<< stfId << std::endl;

        LOG(debug4) << " received message parts size 1 = " << msgParts.At(1)->GetSize() << std::endl;
        struct Roesti::RoestiDataBody *rdpack = reinterpret_cast<Roesti::RoestiDataBody*>(msgParts.At(1)->GetData());
        // LOG(warn) << "--RoestiMagicWord--->" << rdpack->data_length << std::endl;
        LOG(warn) << "---Roesti Data packet--->" << stfHeader << std::endl;
        std::cout << "RD word:" << std::hex << rdpack->data_packet_word << std::endl;
        std::cout << "Data:" << rdpack->data << std::endl;
        
        // rawbuf.reserve(rdpack->data_packet_word);
        // vectorBuffer.reserve(TotalLength(msgParts.At(1)->GetData()));
        // memcpy(pStartBuffer, &rdpack, msgParts.At(1)->GetSize());

        // rawbuf.insert(rawbuf.end(), reinterpret_cast<const char*>(rdpack->data), reinterpret_cast<const char*>(rdpack->data)+ sizeof(rdpack->data));

        // if (fTFBuffer.find(stfId) == fTFBuffer.end()) {
        //     fTFBuffer[stfId].reserve(fNumSource);
        // }
        // fTFBuffer[stfId].emplace_back(STFBuffer {std::move(msgParts), std::chrono::steady_clock::now()});

        // if (!fTFBuffer.empty()) {
        //     LOG(info) << fTFBuffer[stfId];
        //     fTFBuffer.clear();
        // }

        // ---raw buffer variant
        // const auto ptr = reinterpret_cast<unsigned char*>(rdpack);
        // const auto ptr_next = ptr + msgParts.Size();
        // // // LOG(debug4) << "--raw-->" << ptr << std::endl;

        rawbuf.insert(
            rawbuf.end(),
            std::make_move_iterator(reinterpret_cast<char *>(ptr)),
            std::make_move_iterator(reinterpret_cast<char *>(ptr_next)));

        if( fstr.is_open() && fopened ) {
            fstr.write(rawbuf.data(), length);
        }

        // if( fstr.is_open() && fopened ) {
            // fstr.write(reinterpret_cast<const char*>(rdpack), msgParts.At(1)->GetSize());
            // fstr.write(vectorBuffer.data(), vectorBuffer.size()); // 
        // }

    }



    // // bool fSave = true;

    // auto length = TotalLength(msgParts);
    // std::vector<char> rawbuf;
    // rawbuf.reserve(length);
    // std::cout << "Total len for file: "<< length << std::endl;

    // for (const auto& msg : msgParts) {

    // //     if(!fSave) {
    //         // const auto ptr = reinterpret_cast<unsigned char*>(msg->GetData());
    //         //    std::string s(ptr, ptr+msg->GetSize());
    //         //    LOG(debug) << __FUNCTION__ << " received = " << s << " [" << index << "] "
    //         //    << fNumMessages;
    //         //    LOG(debug) << s;

    //         LOG(debug) << __FUNCTION__ << " received = " << " [" << msg->GetData() << "] ";

    //         size_t gsize = msg->GetSize();
    //         std::cout <<"GetSize: " << gsize << std::endl;

    // //         for(size_t i = 0; i < msg->GetSize(); i++) {
    // //             printf("%02x", ptr[i]);
    // //             if( ( (i+1)%8 ) == 0 )
    // //                 printf("\n");
    // //         }

    // //     } else if(fSave) {
    // //         LOG(debug) << __FUNCTION__ << " received = " << " [" << index << "] " << fNumMessages;
    //         const auto ptr = reinterpret_cast<unsigned char*>(msg->GetData());
    //         const auto ptr_next = ptr + msg->GetSize();
    //         rawbuf.insert(rawbuf.end(),
    //                       std::make_move_iterator( reinterpret_cast<char*>(ptr) ),
    //                       std::make_move_iterator( reinterpret_cast<char*>(ptr_next) )
    //                      );

    // //     }// save or print
    //     // ++fNumMessages;
    // }//msg

    // if( fstr.is_open() && fopened ) {
    //     fstr.write(rawbuf.data(), length);
    // }

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


// ________________________________________________________________________________________________________________________Multipart simple example
// #include <chrono>
// #include <thread>

// #include <fairmq/runFairMQDevice.h>

// #include "RoestiSink.h"

// static constexpr std::string_view MyClass{"RoestiSink"};

// namespace bpo = boost::program_options;

// //_____________________________________________________________________________
// void addCustomOptions(bpo::options_description &options)
// {
//     using opt = RoestiSink::OptionKey;
//     options.add_options()
//     (opt::InputChannelName.data(), bpo::value<std::string>()->default_value(opt::InputChannelName.data()), "Name of input channel\n")
//     //
//     (opt::Multipart.data(), bpo::value<std::string>()->default_value("true"), "Handle multipart message\n");
// }

// //_____________________________________________________________________________
// FairMQDevicePtr getDevice(const FairMQProgOptions &)
// {
//     return new RoestiSink;
// }

// //_____________________________________________________________________________
// void PrintConfig(const fair::mq::ProgOptions* config, std::string_view name, std::string_view funcname)
// {
//     auto c = config->GetPropertiesAsStringStartingWith(name.data());
//     std::ostringstream ss;
//     ss << funcname << "\n\t " << name << "\n";
//     for (const auto &[k, v] : c) {
//         ss << "\t key = " << k << ", value = " << v << "\n";
//     }
//     LOG(debug) << ss.str();
// }

// //_____________________________________________________________________________
// bool RoestiSink::HandleData(FairMQMessagePtr &msg, int index)
// {
//     const auto ptr = reinterpret_cast<char*>(msg->GetData());
//     std::string s(ptr, ptr+msg->GetSize());
//     LOG(debug) << __FUNCTION__ << " received = " << s << " [" << index << "] " << fNumMessages;
//     ++fNumMessages;
//     return true;
// }

// //_____________________________________________________________________________
// bool RoestiSink::HandleMultipartData(FairMQParts &msgParts, int index)
// {
//     for (const auto& msg : msgParts) {
//         const auto ptr = reinterpret_cast<char*>(msg->GetData());
//         std::string s(ptr, ptr+msg->GetSize());
//         LOG(debug) << __FUNCTION__ << " received = " << s << " [" << index << "] " << fNumMessages;
//         LOG(debug) << s;
//         ++fNumMessages;
//     }
//     return true;
// }

// //_____________________________________________________________________________
// void RoestiSink::Init()
// {
//     PrintConfig(fConfig, "channel-config", __PRETTY_FUNCTION__);
//     PrintConfig(fConfig, "chans.", __PRETTY_FUNCTION__);

//     fNumMessages = 0;
// }

// //_____________________________________________________________________________
// void RoestiSink::InitTask()
// {
//     PrintConfig(fConfig, "channel-config", __PRETTY_FUNCTION__);
//     PrintConfig(fConfig, "chans.", __PRETTY_FUNCTION__);

//     LOG(debug) << MyClass << " InitTask";
//     using opt = OptionKey;

//     fInputChannelName = fConfig->GetProperty<std::string>(opt::InputChannelName.data());
//     LOG(debug) << " input channel = " << fInputChannelName;

//     const auto &isMultipart = fConfig->GetProperty<std::string>(opt::Multipart.data());
//     if (isMultipart=="true" || isMultipart=="1") {
//         LOG(warn) << " set multipart data handler";
//         OnData(fInputChannelName, &RoestiSink::HandleMultipartData);
//     } else {
//         LOG(warn) << " set data handler";
//         OnData(fInputChannelName, &RoestiSink::HandleData);
//     }

// }


// //_____________________________________________________________________________
// void RoestiSink::PreRun()
// {
//     const auto rnumber = fConfig->GetProperty<std::string>("run_number");
//     LOG(debug) << "run_number: " << rnumber;

//     // char filename[256];
//     // sprintf(filename, "./data/run%06d.dat", stoi(rnumber));
//     // fstr.open(filename, std::ios::binary);
//     // fopened = true;

// }

// //_____________________________________________________________________________
// void RoestiSink::PostRun()
// {
//     using opt = OptionKey;
//     LOG(debug) << __func__;
//     int nrecv=0;
//     while (true) {
//         const auto &isMultipart = fConfig->GetProperty<std::string>(opt::Multipart.data());
//         if (isMultipart=="true" || isMultipart=="1") {
//             FairMQParts parts;
//             if (Receive(parts, fInputChannelName) <= 0) {
//                 LOG(debug) << __func__ << " no data received " << nrecv;
//                 ++nrecv;
//                 if (nrecv>10) {
//                     break;
//                 }
//                 std::this_thread::sleep_for(std::chrono::milliseconds(200));
//             } else {
//                 LOG(debug) << __func__ << " print data";
//                 HandleMultipartData(parts, 0);
//             }
//         } else {
//             FairMQMessagePtr msg(NewMessage());
//             if (Receive(msg, fInputChannelName) <= 0) {
//                 LOG(debug) << __func__ << " no data received " << nrecv;
//                 ++nrecv;
//                 if (nrecv>10) {
//                     break;
//                 }
//                 std::this_thread::sleep_for(std::chrono::milliseconds(200));
//             } else {
//                 LOG(debug) << __func__ << " print data";
//                 HandleData(msg, 0);
//             }
//         }
//         LOG(debug) << __func__ << " done";
//     }
// }