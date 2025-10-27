
#include <cstring>
#include <fairmq/Device.h>
#include <fairmq/runDevice.h>

#include <iostream>
#include <iterator>
#include <ostream>
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

#include "SubTimeFrameHeader.h"
// #include "SubTimeFrameHeader.h"
#include "roesti.h"

#include "CliSock.cxx"
#include "RBCP.cxx"
#include "KTimer.cxx"
#include "hexdump.cxx"
#include "utility/HexDump.h"
#include "utility/Hexdump.hpp"

// #define LOG(val) std::cout
// int fTimeout_ms = 0;
// std::string fDeviceIp = "192.168.10.19";
// int fDataPort = 24;
// int fControlPort = 4660;
// int fNumDestination = 1;

// CliSock fSock;

char* buffwrap(unsigned char value){
	static unsigned char buffer_value[4] = {0x00, 0x00, 0x00, 0x00};
	buffer_value[0] = value;
	return reinterpret_cast<char *>(buffer_value);
  
}

char* buffcast(uint8_t num){
	static char* bytePtr = reinterpret_cast<char*>(&num);
	return bytePtr;
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

	// int err_board=1;
	// unsigned int length[256];

	RBCP rbcp;

	if(rbcp.Open(fDeviceIp.c_str(), fControlPort)){

	LOG(info) << "Opened IP: " << fDeviceIp << "Port" << fControlPort;
	// LOG(info) << "Run Mode: " << fMode;
	

	LOG(info) << "Opened IP: " << fDeviceIp << "Port" << fControlPort;
	LOG(info) << "-----SENDIND INIT COMMANDS-------";
	
	//RESET

	if (rbcp.Write(buffcast(0x00), 0x16a, 1) > 0) { LOG(info) << "-----SET: 0x16a"; }
	if (rbcp.Write(buffcast(0x00), 0x11d, 1) > 0) { LOG(info) << "-----SET: 0x11d"; }
	if (rbcp.Write(buffcast(0xf0), 0x110, 1) > 0) { LOG(info) << "-----SET: 0x110"; }
	if (rbcp.Write(buffcast(0x01), 0x112, 1) > 0) { LOG(info) << "-----SET: 0x112"; }

	//SETUP
	if(rbcp.Write( buffwrap(0xf0), RESET_INTR          , 1) > 0){ LOG(info) << "-----SET: Reset"; }	
	if(rbcp.Write( buffwrap(31)  , DRS4_SAMPLING_SPEED , 1) > 0){ LOG(info) << "-----SET: DRS4_SAMPLING_SPEED"; }
	if(rbcp.Write( buffwrap(0x08), ADC_RESET_SYNC      , 1) > 0){ LOG(info) << "-----SET: ADC_RESET_SYNC"; }
	if(rbcp.Write( buffwrap(0x06), ADC_OUT_SYNC        , 1) > 0){ LOG(info) << "-----SET: ADC_OUT_SYNC"; }
	if(rbcp.Write( buffwrap(0x3C), ADC_RESET_CH07      , 1) > 0){ LOG(info) << "-----SET: ADC_RESET_CH07"; }
	if(rbcp.Write( buffwrap(0x3C), ADC_RESET_CH815     , 1) > 0){ LOG(info) << "-----SET: ADC_RESET_CH815"; }
	if(rbcp.Write( buffwrap(0x03), ADC_PWM_CH07        , 1) > 0){ LOG(info) << "-----SET: ADC_PWM_CH07"; }
	if(rbcp.Write( buffwrap(0x00), ADC_PWM_CH07        , 1) > 0){ LOG(info) << "-----SET: ADC_PWM_CH07_2"; }
	if(rbcp.Write( buffwrap(0x03), ADC_PWM_CH815       , 1) > 0){ LOG(info) << "-----SET: ADC_PWM_CH815"; }
	if(rbcp.Write( buffwrap(0x00), ADC_PWM_CH815       , 1) > 0){ LOG(info) << "-----SET: ADC_PWM_CH815_2"; }
	if(rbcp.Write( buffwrap(0x00), ADC_OUT_CH07        , 1) > 0){ LOG(info) << "-----SET: ADC_OUT_CH07 "; }
	if(rbcp.Write( buffwrap(0x00), ADC_OUT_CH815       , 1) > 0){ LOG(info) << "-----SET: ADC_OUT_CH815"; }
	if(rbcp.Write( buffwrap(0x00), ADC_TEST_CH07       , 1) > 0){ LOG(info) << "-----SET: ADC_TEST_CH07"; }
	if(rbcp.Write( buffwrap(0x00), ADC_TEST_CH815      , 1) > 0){ LOG(info) << "-----SET: ADC_TEST_CH815"; }
	if(rbcp.Write( buffwrap(0x08), THR_UPP             , 1) > 0){ LOG(info) << "-----SET: THR_UPP"; }
	if(rbcp.Write( buffwrap(0x00), THR_LOW             , 1) > 0){ LOG(info) << "-----SET: THR_LOW"; }
	if(rbcp.Write( buffwrap(0x08), TRIG                , 1) > 0){ LOG(info) << "-----SET: TRIG_TYPE"; } // 0x00-off, 0x02-base, 0x04-self, 0x08-normal
	// if(rbcp.Write( buffwrap(0x00), ADC_OUT_PHASE_CH07  , 1) > 0){ LOG(info) << "-----SET: ADC_OUT_PHASE_CH07"; }
	   // if(rbcp.Write( buffwrap(0x00), ADC_OUT_PHASE_CH815 , 1) > 0){ LOG(info) << "-----SET: ADC_OUT_PHASE_CH815"; }
	// if(rbcp.Write( buffwrap(0x3c), ASD_CHECK_START_TIME, 1) > 0){ LOG(info) << "-----SET: ASD_CHECK_START_TIME"; } // ?
	// if(rbcp.Write( buffwrap(0x46), ASD_WAIT_WIDTH      , 1) > 0){ LOG(info) << "-----SET: ASD_WAIT_WIDTH"; } // ?
	if(rbcp.Write( buffwrap(60), ASD_CHECK_START_TIME  , 1) > 0){ LOG(info) << "-----SET: ASD_CHECK_START_TIME"; } // ?
	if(rbcp.Write( buffwrap(70), ASD_WAIT_WIDTH        , 1) > 0){ LOG(info) << "-----SET: ASD_WAIT_WIDTH"; } // ?
	if(rbcp.Write( buffwrap(0x00), ZSUP                , 1) > 0){ LOG(info) << "-----SET: ZSUP"; } // 0x01-on, 0x00-off
	if(rbcp.Write( buffwrap(0x01), DSEND               , 1) > 0){ LOG(info) << "-----SET: DSEND"; } // 0x01-on, 0x00-off
	if(rbcp.Write( buffwrap(0x01), DAC_SYNC            , 1) > 0){ LOG(info) << "-----SET: DAC_SYNC"; }
	if(rbcp.Write( buffwrap(0xf0), RESET_DRS           , 1) > 0){ LOG(info) << "-----SET: RESET_DRS"; }
	if(rbcp.Write( buffwrap(0xf0), RESET_INTR          , 1) > 0){ LOG(info) << "-----SET: RESET_INTR"; }
	if(rbcp.Write( buffwrap(0x2F), DATA_LAT            , 1) > 0){ LOG(info) << "-----SET: DATA_LAT"; }
	if(rbcp.Write( buffwrap(0x01), DATA_RESET          , 1) > 0){ LOG(info) << "-----SET: DATA_RESET"; } //emties ADC data
	if(rbcp.Write( buffwrap(0x04), ADC_OUT_PHASE_CH07  , 1) > 0){ LOG(info) << "-----SET: ADC_OUT_PHASE_CH07"; }
	if(rbcp.Write( buffwrap(0x04), ADC_OUT_PHASE_CH815 , 1) > 0){ LOG(info) << "-----SET: ADC_OUT_PHASE_CH815"; }
	if(rbcp.Write( buffwrap(0x00), DAISY_CHAIN         , 1) > 0){ LOG(info) << "-----SET: DAISY_CHAIN"; }

	// TCP_SET
	// // if(rbcp.Write( buffwrap(0x16a),0x01, 1) > 0){ LOG(info) << "-----SET: TCP"; }
	// // if(rbcp.Write( buffwrap(0x16D),0x01, 1) > 0){ LOG(info) << "-----SET: Check TCP"; }

	}else{
		LOG(error) << "Error in connection";
		
	}
	
	rbcp.Close();
	// // fKt1.SetDuration(5000);

}


bool CheckRoestiHeader(char *buf){ return true; }

int CheckRoestiDataPacket(unsigned char *data, int len)
{

	unsigned char *buf = data;
	
	while(true) {
		struct Roesti::DataPacketHeader *ppheader = reinterpret_cast<Roesti::DataPacketHeader *>(buf);
		if (ppheader->data_packet_word != Roesti::DATA_PACKET_MAGIC) break;

		#ifdef DEBUG
			std::cout << "DP MAGIC: 0x" << std::hex << ppheader->data_packet_word;
			std::cout << ", res: 0x" << std::hex << static_cast<unsigned int>(ppheader->reserved_header);
			std::cout << ", ch: " << std::dec << static_cast<unsigned int>(ppheader->channel_num);
			std::cout << ", stop: " << std::dec << ppheader->stop_num;
			std::cout << ", Len: " << std::dec << ppheader->data_length;
			std::cout << std::endl;
		#endif

		struct Roesti::DataPacketFooter *ppfooter
			= reinterpret_cast<Roesti::DataPacketFooter *>
			(buf + ppheader->data_length + sizeof(Roesti::DataPacketHeader));
		#ifdef DEBUG
			std::cout << "DP footer: 0x" << std::hex << ppfooter->footer;
			std::cout << ", keyword: 0x" << static_cast<unsigned int>(ppfooter->keyword);
			std::cout << ", res: 0x" << static_cast<unsigned int>(ppfooter->reserved);
			std::cout << std::dec << std::endl;
		#endif

		buf += sizeof(Roesti::DataPacketHeader) + ppheader->data_length + sizeof(Roesti::DataPacketFooter);
		if ((buf - data) > len) break;
	}

	#ifdef DEBUG
		int left = buf - data - len;
		if (left > 0) {
			std::cout << "Left data: " << left << " (" << (buf - data) << "/" << len << ")";
			std::for_each(buf, data + len, HexDump(16));
		}
	#endif

	return 0;
}



bool RoestiSampler::ConditionalRun()
{
	
	fair::mq::Parts outParts;
	// struct Roesti::RoestiHeader roesti_header;

	// NestDAQ header format here first
	// outParts.AddPart(NewMessage(sizeof(struct Roesti::RoestiHeader)));

	// outParts.AddPart(NewMessage(sizeof(recv_header) + body_packet));
	// //auto &msgRBody = parts[2];
	// auto &msg = outParts[1];
	// fair::mq::Parts outParts;
	// outParts.AddPart(NewMessage(body_read));


	int nflag;

	//---Read header

	unsigned char rec_buf[40*1000];
	struct Roesti::RoestiHeader *pheader = reinterpret_cast<struct Roesti::RoestiHeader *>(rec_buf);
	int header_size = sizeof(struct Roesti::RoestiHeader);

	int header_read = fSock.Receive(reinterpret_cast<char *>(pheader), sizeof(struct Roesti::RoestiHeader) , nflag);

	// LOG(info) << "Header data recieved: " << header_read;
	// LOG(warn) << "Size of defined nhsize : " << sizeof(nhsize);

	// char ev[4];
	// memcpy(&ev[0], &pheader[12], 4);
    // std::cout << "-------------ev manual--------->"<< std::hex << ev << std::endl;

	
	if (header_read < header_size) {
		LOG(warn) << "Irregal header data size : " << header_read << "/" << header_size;
		if (nflag == EAGAIN) {
			LOG(warn) << "Timeout and Retry";
			int hh = fSock.Receive(reinterpret_cast<char *>(pheader) + header_read,
				 header_size - header_read, nflag);
			header_read += hh;
			if (header_read < header_size) {
				if (nflag == EAGAIN) {
					std::cout << "Timeout anagin..." << std::endl;
				} else {
					std::cout << "Unknown error..." << std::endl;
				}
			}
		} else {
			return true;
		}
	}

	#ifdef DEBUG
		std::cout<< "--------------------HEADER--------------------" << std::endl;
		std::cout << "body length size: " << sizeof(pheader->length)
			<< " / body length value: " << pheader->length << std::endl;
		// std::cout << "event number: " << pheader->event_num << std::endl;

		std::cout << "MAGIC: 0x" << std::hex << pheader->magic;
		std::cout << ", Type: 0x" << std::hex << pheader->type;
		std::cout << ", IP: " << std::dec << (pheader->ipaddr & 0xff00 >> 16)
			<< "." << (pheader->ipaddr & 0xff) << std::endl;
		std::cout << " Len: " << std::dec << pheader->length;
		std::cout << " SEU: " << std::dec << pheader->seu_count;
		std::cout << " SEU size: " << std::dec << pheader->seu_inf_size;
		std::cout << " MBU: " << std::dec << pheader->mbu_count;
		std::cout << ", Event Number: " << (pheader->event_num & 0xff) << std::endl;
	#endif

	//---Read data packet
	unsigned char *TCP_data = rec_buf + sizeof(struct Roesti::RoestiHeader);
	
	// int body_size = static_cast<int>(ntohs(pheader->length));
	// // int body_size = static_cast<int>(ntohs(TCP_data));

	// outParts.AddPart(NewMessage(body_size+header_size));
	// auto &msgRoesti = outParts[1];

	// char *cmsgbuf = reinterpret_cast<char *>(msgRoesti.GetData());
	// memcpy(cmsgbuf, reinterpret_cast<char *>(pheader), sizeof(struct Roesti::RoestiHeader));
	// unsigned char *roesti_data = reinterpret_cast<unsigned char *>(TCP_data);


	unsigned int dp_read = 0;
	unsigned int dp_len = pheader->length;

	// for (int i = 0 ; i < 10 ; i++) {
		dp_read += fSock.Receive(reinterpret_cast<char *>(TCP_data), dp_len - dp_read, nflag);
		
		#ifdef DEBUG
			std::cout<< "--------------------DATA--------------------" << std::endl;
			std::cout << "data packet read/length: " << dp_read << "/" << dp_len << std::endl;
		#endif
		
		// if (dp_read >= dp_len) {
		// 	LOG(warn) << "Length check: " << dp_read << "/" << dp_len;
		// }else 
		if (dp_read < dp_len){
			LOG(error) << "Insufficent size of data";
		}else{
			if (nflag == EAGAIN) {
				LOG(warn) << "Timeout and Retry";
				dp_read += fSock.Receive(reinterpret_cast<char *>(TCP_data), dp_len - dp_read, nflag);
			}
			// else {
			// 	LOG(warn) << "Length check: " << dp_read << "/" << dp_len;
			// 	// std::cout << "Unknown error... at data reading" << std::endl;
			// 	// return true;
			// }
		}

	//---custom hexdump into file from stream
	// hexdump(reinterpret_cast<char *>(&TCP_data), dp_read);
	// std::ofstream out("./dataout/procout.dat");
	// std::streambuf *coutbuf = std::cout.rdbuf(); //save old buf
	// std::cout.rdbuf(out.rdbuf()); //redirect std::cout to out.txt!
	// std::cout << CustomHexdump<16, false>(&TCP_data, sizeof(TCP_data)) << std::endl;
	// std::cout.rdbuf(coutbuf);

	//--- Unpack data packet

	CheckRoestiDataPacket(TCP_data, dp_read);
	
	// check buffer with output to file
	// std::ofstream outputFile("./dataout/datapacket.bin", std::ios::binary); 
	// outputFile.write(reinterpret_cast<char *>(TCP_data), dp_len);
	// // outputFile.write(reinterpret_cast<char *>(pheader),sizeof(struct Roesti::RoestiHeader));
    // outputFile.close();

	//---Read footer

	int footer_size = 8;
	char recvbuff[16];
	struct Roesti::RoestiFooter *pfooter = reinterpret_cast<Roesti::RoestiFooter *>(recvbuff);
	int footer_read = fSock.Receive(reinterpret_cast<char *>(&recvbuff), footer_size, nflag);

	#ifdef DEBUG
		std::cout<< "--------------------FOOTER--------------------" << std::endl;
		std::cout << "Footer: size:" << std::dec << footer_read
			<< ", key: 0x" << std::hex << pfooter->footer
			<< ", crc: 0x" << std::hex << pfooter->crc32 << std::endl;
		std::cout << std::dec;
	#endif

    //This needs to become full packet send
	// outParts.AddPart(NewMessage(sizeof(struct Roesti::RoestiHeader)));

	//---stf
	outParts.AddPart(NewMessage(sizeof(SubTimeFrame::Header)));
	auto &msgSTFHeader = outParts[0];

	struct SubTimeFrame::Header *pstfheader = reinterpret_cast<struct SubTimeFrame::Header *>(msgSTFHeader.GetData());

	pstfheader->magic = SubTimeFrame::Magic;
	pstfheader->timeFrameId = pheader->event_num;
	pstfheader->FEMType = static_cast<uint32_t>(pheader->type) & 0xff;
	pstfheader->FEMId = static_cast<uint32_t>(pheader->ipaddr) & 0xff;
	// pstfheader->length = sizeof(struct SubTimeFrame::Header) + sizeof(struct Roesti::RoestiHeader)+sizeof(rec_buf);
	pstfheader->length =  pheader->length;//dp_len; //sizeof(struct SubTimeFrame::Header);
	pstfheader->numMessages = 1;
	struct timeval now;
	gettimeofday(&now, nullptr);
	pstfheader->time_sec = now.tv_sec;
	pstfheader->time_usec = now.tv_usec;

	// std::cout << "-------STF HEADER VALUES--------"<< std::endl;
	// std::cout << "Magic:" << std::hex << pstfheader->magic << std::endl;
	// std::cout << "Event:" << std::dec << pstfheader->timeFrameId << std::endl;
	// std::cout << "IP" << std::dec << pstfheader->FEMId << std::endl;


	//--roesti header+ datapacket exact
	unsigned int fullPacketSize = sizeof(struct Roesti::RoestiHeader) + sizeof(Roesti::DataPacketHeader) 
	                              + pheader->length + sizeof(Roesti::DataPacketFooter);
	outParts.AddPart(NewMessage(fullPacketSize));

	// outParts.AddPart(NewMessage(sizeof(rec_buf)));
	auto &msgRoesti = outParts[1];

	char *msgbuf = reinterpret_cast<char *>(msgRoesti.GetData());
	// memcpy(msgbuf, rec_buf, sizeof(rec_buf));
	memcpy(msgbuf, pheader, sizeof(struct Roesti::RoestiHeader));
	msgbuf+=sizeof(struct Roesti::RoestiHeader);
	memcpy(msgbuf, TCP_data, dp_len);

	
	// char *body_packet = reinterpret_cast<char *>(msgRoesti.GetData());
	// memcpy(body_packet, reinterpret_cast<char *>(&TCP_data), pheader->length);

	// struct Roesti::DataPacketHeader *ppheader = reinterpret_cast<Roesti::DataPacketHeader *>(msgRoesti.GetData());
	// ppheader = reinterpret_cast<struct Roesti::DataPacketHeader *>(TCP_data);
	
	// struct Roesti::RoestiHeader *rhead = reinterpret_cast<struct Roesti::RoestiHeader *>(msgRoesti.GetData());
    // memcpy(rhead, pheader, header_size);
	
	// struct Roesti::RoestiDataBody *roestidata = reinterpret_cast<struct Roesti::RoestiDataBody *>(msgRoesti.GetData());
	// memcpy(roestidata, TCP_data, dp_read);

	// struct Roesti::RoestiFooter *rfoot = reinterpret_cast<struct Roesti::RoestiFooter *>(msgRoesti.GetData());
	// memcpy(rfoot, pfooter, footer_size);


	// Send all outParts
	Send(outParts, fOutputChannelName);
	#ifdef DEBUG
		LOG(warn) << "PACKET sent";
	#endif

	return true;
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
	if (fTimeout_ms > 0) fSock.SetTimeOut_ms(fTimeout_ms);
	// fSock.SetBuffer(33554432);
	int sock = fSock.Connect(fDeviceIp.c_str(), fDataPort);
	if (sock < 0) {
		LOG(error) << "Connection err. " << fDeviceIp << " " << fDataPort;
		//return false;
		// return;
	}
	//return true;
	return;
}


void RoestiSampler::Run()
{
	// LOG(debug) << __FUNCTION__;
}

