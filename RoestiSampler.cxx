
#include <fairmq/Device.h>
#include <fairmq/runDevice.h>

#include <iostream>
#include <string>
#include <chrono>
#include <cstdint>

#include <thread> // this_thread::sleep_for
// #include "Header.h"

#include <sys/socket.h>
#include <arpa/inet.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <signal.h>

// #include "SubTimeFrameHeader.h"
#include "roesti.h"

#include "CliSock.cxx"
#include "RBCP.cxx"
#include "KTimer.cxx"
// #include "hexdump.cxx"


char* buffwrap(unsigned char value){
	static char buffer_value[4];
	buffer_value[0] = value;
	return buffer_value;
}

namespace bpo = boost::program_options;

class RoestiSampler : public fair::mq::Device
{
public:
	struct OptionKey {
		static constexpr std::string_view DeviceIp	        {"ip"};
		static constexpr std::string_view DataPort	        {"port"};
		static constexpr std::string_view Timeout_ms        {"timeout"};
		static constexpr std::string_view ControlPort       {"control-port"};
		static constexpr std::string_view OutputChannelName {"out-chan-name"};
		static constexpr std::string_view DQMChannelName    {"dqm-chan-name"};
		static constexpr std::string_view Mode              {"mode"};
		static constexpr std::string_view PollTimeout       {"poll-timeout"};

		static constexpr std::string_view RBCP              {"rbcp"};
	};

	RoestiSampler() : fair::mq::Device() {};
	RoestiSampler(const RoestiSampler&)	    = delete;
	RoestiSampler& operator=(const RoestiSampler&) = delete;
	~RoestiSampler() = default;

protected:
	void Init() override;
	void InitTask() override;
	void PreRun() override;
	bool ConditionalRun() override;
	void Run() override;
	void PostRun() override;
	//bool PreRun() override;
	bool CheckRoestiHeader(char*);

private:
	uint64_t fNumIterations = 4;
	unsigned int fNumDestination = 0;
	unsigned int fPollTimeoutMS = 0;
	CliSock fSock;
	std::string fOutputChannelName;
	std::string fDQMChannelName;
	std::string fDeviceIp     {"192.168.10.19"};
	unsigned int fDataPort   = 24;
	unsigned int fControlPort = 4660;
	unsigned int fDeviceType  = 0x100;
	unsigned int fTimeout_ms = 0;
	unsigned int fMode = 0;
	//simple send
	std::string fText;
    uint64_t fMaxIterations = 10;
	//--
	KTimer fKt1;
};


void addCustomOptions(bpo::options_description& options)
{
	using opt = RoestiSampler::OptionKey;
	options.add_options()
		("max-iterations",
			bpo::value<uint64_t>()->default_value(5),
			"Maximum number of iterations of Run/ConditionalRun/OnData (0 - infinite)")
		(opt::OutputChannelName.data(),
			bpo::value<std::string>()->default_value("out"),
			"Name of the output channel")
		(opt::DQMChannelName.data(),
			bpo::value<std::string>()->default_value("dqm"),
			"Name of the data quality monitoring channel")
		(opt::DeviceIp.data(),
			bpo::value<std::string>()->default_value("192.168.10.19"),
			"IP-address of the front-end device")
		(opt::DataPort.data(),
			bpo::value<std::string>()->default_value("24"),
			"Data port of the front-end device")
		(opt::ControlPort.data(),
			bpo::value<std::string>()->default_value("4660"),
			"Control port of the front-end deive")
		(opt::Timeout_ms.data(),
			bpo::value<std::string>()->default_value("50000"),
			"Timeout of the front-end deive")
		(opt::Mode.data(),
			bpo::value<std::string>()->default_value("0x00"),
			"Roesti run mode")
		(opt::PollTimeout.data(),
			bpo::value<std::string>()->default_value("1"),
			"Timeout of polling (in msec)")

		(opt::RBCP.data(),
			bpo::value<std::vector<std::string> >()->default_value(
				{}, "address, value, ..."),
				"Configuation registers")
			#if 0
			bpo::value<std::vector<std::string> >()->default_value(
				{"0x05", "0x01", "0x06", "0x20"}, "address, value, ..."),
				"Configuation registers")
			#endif
		;
}


std::unique_ptr<fair::mq::Device> getDevice(fair::mq::ProgOptions& /*config*/)
{
	return std::make_unique<RoestiSampler>();
}


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


void RoestiSampler::Init()
{
	LOG(debug) << __FUNCTION__;
}


// void RoestiSampler::Setup(unsigned char buffer, unsigned int address)
// {
// 	LOG(debug) << __FUNCTION__;
// }

void RoestiSampler::InitTask()
{
	LOG(debug) << __FUNCTION__;

	PrintConfig(fConfig, "channel-config", __PRETTY_FUNCTION__);
	PrintConfig(fConfig, "chans.", __PRETTY_FUNCTION__);

	using opt = OptionKey;
	fOutputChannelName = fConfig->GetProperty<std::string>(opt::OutputChannelName.data());
	fDQMChannelName    = fConfig->GetValue<std::string>(opt::DQMChannelName.data());

	fDeviceIp          = fConfig->GetProperty<std::string>(opt::DeviceIp.data());
	fDataPort          = std::stoi(fConfig->GetProperty<std::string>(opt::DataPort.data()));
	fControlPort       = std::stoi(fConfig->GetProperty<std::string>(opt::ControlPort.data()));
	fTimeout_ms        = std::stoi(fConfig->GetProperty<std::string>(opt::Timeout_ms.data()));
	fMode              = std::stoi(fConfig->GetProperty<std::string>(opt::Mode.data()));

	fPollTimeoutMS     = std::stoi(fConfig->GetProperty<std::string>(opt::PollTimeout.data()));
	fNumDestination    = GetNumSubChannels(fOutputChannelName);

	LOG(info) << "IP Address: " << fDeviceIp
		<< ", Port: " << fDataPort << ", Control Port: " << fControlPort
		<< "  Number of destinations: " << fNumDestination;

	//fDeviceType	= std::stoi(fConfig->GetProperty<std::string>("DeviceType"));
	//LOG(info) << "Device Type: " << fDeviceType;

	int err_board=1;
	unsigned int length[256];

	RBCP rbcp;

	if(rbcp.Open(fDeviceIp.c_str(), fControlPort)){

	LOG(info) << "Opened IP: " << fDeviceIp << "Port" << fControlPort;
	LOG(info) << "Run Mode: " << fMode;
	

	LOG(info) << "Opened IP: " << fDeviceIp << "Port" << fControlPort;
	LOG(info) << "-----SENDIND INIT COMMANDS-------";
	
	//RESET

	if (rbcp.Write(buffwrap(0x00), 0x16a, 1) > 0) { LOG(info) << "-----SET: 0x16a"; }
	if (rbcp.Write(buffwrap(0x00), 0x11d, 1) > 0) { LOG(info) << "-----SET: 0x11d"; }
	if (rbcp.Write(buffwrap(0xf0), 0x110, 1) > 0) { LOG(info) << "-----SET: 0x110"; }
	if (rbcp.Write(buffwrap(0x01), 0x112, 1) > 0) { LOG(info) << "-----SET: 0x112"; }

	//SETUP

	if(rbcp.Write( buffwrap(31)  , DRS4_SAMPLING_SPEED , 1) > 0){ LOG(info) << "-----SET: DRS4_SAMPLING_SPEED"; }
	if(rbcp.Write( buffwrap(0x08), ADC_RESET_SYNC      , 1) > 0){ LOG(info) << "-----SET: ADC_RESET_SYNC"; }
    if(rbcp.Write( buffwrap(0x06), ADC_OUT_SYNC        , 1) > 0){ LOG(info) << "-----SET: ADC_OUT_SYNC"; }
    if(rbcp.Write( buffwrap(0x3C), ADC_RESET_CH07      , 1) > 0){ LOG(info) << "-----SET: ADC_RESET_CH07"; }
    if(rbcp.Write( buffwrap(0x3C), ADC_RESET_CH815     , 1) > 0){ LOG(info) << "-----SET: ADC_RESET_CH815"; }
    if(rbcp.Write( buffwrap(0x03), ADC_PWM_CH07        , 1) > 0){ LOG(info) << "-----SET: ADC_PWM_CH07"; }
    if(rbcp.Write( buffwrap(0x00), ADC_PWM_CH815       , 1) > 0){ LOG(info) << "-----SET: ADC_PWM_CH815"; }
    if(rbcp.Write( buffwrap(0x03), ADC_OUT_CH07        , 1) > 0){ LOG(info) << "-----SET: ADC_OUT_CH07"; }
    if(rbcp.Write( buffwrap(0x00), ADC_OUT_CH815       , 1) > 0){ LOG(info) << "-----SET: ADC_OUT_CH815"; }
    if(rbcp.Write( buffwrap(0x00), ADC_TEST_CH07       , 1) > 0){ LOG(info) << "-----SET: ADC_TEST_CH07"; }
    if(rbcp.Write( buffwrap(0x00), ADC_TEST_CH815      , 1) > 0){ LOG(info) << "-----SET: ADC_TEST_CH815"; }
    if(rbcp.Write( buffwrap(0x00), ADC_OUT_PHASE_CH07  , 1) > 0){ LOG(info) << "-----SET: ADC_OUT_PHASE_CH07"; }
    if(rbcp.Write( buffwrap(0x00), ADC_OUT_PHASE_CH815 , 1) > 0){ LOG(info) << "-----SET: ADC_OUT_PHASE_CH815"; }
    if(rbcp.Write( buffwrap(0x00), TRIG                , 1) > 0){ LOG(info) << "-----SET: TRIG"; } // 0x00-off, 0x02-base, 0x04-self, 0x08-normal
    if(rbcp.Write( buffwrap(60)  , ASD_CHECK_START_TIME, 1) > 0){ LOG(info) << "-----SET: ASD_CHECK_START_TIME"; } // ?
    if(rbcp.Write( buffwrap(70)  , ASD_WAIT_WIDTH      , 1) > 0){ LOG(info) << "-----SET: ASD_WAIT_WIDTH"; } // ?
    if(rbcp.Write( buffwrap(0x01), ZSUP                , 1) > 0){ LOG(info) << "-----SET: ZSUP"; } // 0x01-on, 0x00-off
    if(rbcp.Write( buffwrap(0x01), DSEND               , 1) > 0){ LOG(info) << "-----SET: DSEND"; } // 0x01-on, 0x00-off
    if(rbcp.Write( buffwrap(0x01), DAC_SYNC            , 1) > 0){ LOG(info) << "-----SET: DAC_SYNC"; }
    if(rbcp.Write( buffwrap(0xf0), RESET_DRS           , 1) > 0){ LOG(info) << "-----SET: RESET_DRS"; }
    if(rbcp.Write( buffwrap(0xf0), RESET_INTR          , 1) > 0){ LOG(info) << "-----SET: RESET_INTR"; }
    if(rbcp.Write( buffwrap(0x2F), DATA_LAT            , 1) > 0){ LOG(info) << "-----SET: DATA_LAT"; }
    if(rbcp.Write( buffwrap(0x01), DATA_RESET          , 1) > 0){ LOG(info) << "-----SET: DATA_RESET"; } //emties ADC data
    if(rbcp.Write( buffwrap(0x04), ADC_OUT_PHASE_CH07  , 1) > 0){ LOG(info) << "-----SET: ADC_OUT_PHASE_CH07"; }
    if(rbcp.Write( buffwrap(0x04), ADC_OUT_PHASE_CH815 , 1) > 0){ LOG(info) << "-----SET: ADC_OUT_PHASE_CH815"; }
    if(rbcp.Write( buffwrap(0x153),DAISY_CHAIN         , 1) > 0){ LOG(info) << "-----SET: DAISY_CHAIN"; }

	// TCP_SET
	if(rbcp.Write( buffwrap(0x16a),0x01, 1) > 0){ LOG(info) << "-----SET: TCP"; }
	if(rbcp.Write( buffwrap(0x16D),0x01, 1) > 0){ LOG(info) << "-----SET: Check TCP"; }

	}else{
		LOG(error) << "Error in connection";
		
	}
	
	rbcp.Close();
	fKt1.SetDuration(5000);

}


bool RoestiSampler::CheckRoestiHeader(char *buf)
{

}


bool RoestiSampler::ConditionalRun()
{
	
	int n;
	char recvbuff[1024];
    char recv_header[24];

	int HEADERSIZE = 24;
    int FOTTERSIZE = 8;
    int data_read_size = 0;
    int SEUinfo_read_size = 0;

    union EventNumber_f{
        char EventNumber_char[4];
        unsigned EventNumber_int;
    }EventNumber;

    union Length_f{
        char Length_char[4];
        unsigned Length_int;
    }Length;

    union SEUinfoSize_f{
        char SEUinfoSize_char[2];
        unsigned short SEUinfoSize_int;
    }SEUinfoSize;

    
    union StopNumber_f{
        char StopNumber_char[2];
        unsigned short StopNumber_short;
    }StopNumber;

    char recvbuff_fotter[1024];

	unsigned char TCP_data[37112];
    union f_data{
        unsigned char data_char[2];
        unsigned short data_short;
    }data;


	fair::mq::Parts outParts;
	outParts.AddPart(NewMessage(HEADERSIZE));
	auto &msgHeader = outParts[0];

	int header_read = 0;
	int flag;

	

	int nread = fSock.Receive(reinterpret_cast<char *>(recv_header + header_read), HEADERSIZE - header_read, flag);
	// auto nread = recv(sock, recv_header + header_read, HEADERSIZE - header_read, 0);
	LOG(info) << "Datarecieved -----------\""<< nread << "\"--";
	LOG(info) << "Recieved Header -----------\""<< recv_header << "\"--";
	LOG(info) << "Header read -----------\""<< header_read << "\"--";
	// header_read = header_read + nread;
	// if(header_read == HEADERSIZE) LOG(info) << "Header max reached -----------";
	// if(nread < 0) LOG(info) << "-1 header -----------";
	
	//read start
    

    // n = recv(fSock, recv_header + header_read, HEADERSIZE - header_read, 0);
    // //   fwrite(recv_header + header_read, 1, n, fp);
    //   header_read = header_read + n;
    //   if(header_read == HEADERSIZE) LOG(info) << "Header max reached -----------";

    // memcpy(&EventNumber.EventNumber_char[0], &recv_header[12], 4);
    // memcpy(&Length.Length_char[0], &recv_header[8], 4);
    // memcpy(&SEUinfoSize.SEUinfoSize_char[0], &recv_header[20], 2);

    // int board_IP = (int)recv_header[6];

	// auto text = *nread;

	// FairMQMessagePtr msg(NewMessage(
	// // 	const_cast<char *>(text->data()),
	// // 	text->length(),
	// // 	[](void * /*data*/, void *object)
	// // 	{
	// // 		auto p = reinterpret_cast<std::string *>(object);
	// // 		// LOG(debug) << " sent " << *p;
	// // 		delete p;
	// // 	},
	// nread));

	// LOG(info) << "Sending \"" << nread << "\"";
	//  LOG(info) << "Sending data end--------------";

    // if (Send(recv_header, fOutputChannelName, 0 ) < 0) {
    //     LOG(warn) << "failed to send." ;
    //     return false;
    // }


	// fText = "___TEST MSG___";
	// auto text = new std::string(fConfig->GetProperty<std::string>("id") + ":" + fText + " : " + std::to_string(fNumIterations));

	// // copy
	// auto txt = *text;

	// FairMQMessagePtr msg(NewMessage(
	// 	const_cast<char *>(text->data()),
	// 	text->length(),
	// 	[](void * /*data*/, void *object)
	// 	{ 
    //     auto p = reinterpret_cast<std::string*>(object);
    //     //LOG(debug) << " sent " << *p;
    //     delete p; },
	// 	text));

	// LOG(info) << "Sending \"" << txt << "\"";

	// if (Send(msg, fOutputChannelName) < 0)
	// {
	// 	LOG(warn) << " event:  " << fNumIterations;
	// 	return false;
	// }
	// else
	// {
	// 	++fNumIterations;
	// 	if (fMaxIterations > 0 && fNumIterations >= fMaxIterations)
	// 	{
	// 		LOG(info)
	// 			<< "Configured maximum number of iterations reached. Leaving RUNNING state. "
	// 			<< fNumIterations << " / " << fMaxIterations;
	// 		return false;
	// 	}
	// }

	// LOG(info) << " processed events:  " << fNumIterations;

	// return true;

	
}


void RoestiSampler::PostRun()
{
// 	LOG(debug) << __FUNCTION__;
// 	fNumIterations = 0;
	fSock.Close();
}


//bool RoestiSampler::PreRun()
void RoestiSampler::PreRun()
{
	// LOG(debug) << __FUNCTION__;
	// if (fTimeout_ms > 0) fSock.SetTimeOut_ms(fTimeout_ms);
	// int sock = fSock.Connect(fDeviceIp.c_str(), fDataPort);
	// if (sock < 0) {
	// 	LOG(error) << "Connection err. " << fDeviceIp << " " << fDataPort;
	// 	//return false;
	// 	return;
	// }
	// //return true;
	// return;
}


void RoestiSampler::Run()
{
	// LOG(debug) << __FUNCTION__;
}
