#include <cstring>
#include <fairmq/Device.h>
#include <fairmq/runDevice.h>

#include <iostream>
#include <iterator>
#include <ostream>
#include <string>
#include <chrono>
#include <cstdint>


#include <sys/socket.h>
#include <arpa/inet.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <signal.h>
#include <hiredis/hiredis.h>
#include <unistd.h>

#include "SubTimeFrameHeader.h"
#include "roesti.h"

#include "CliSock.cxx"
#include "RBCP.cxx"
#include "KTimer.cxx"
#include "hexdump.cxx"
#include "utility/HexDump.h"
#include "utility/Hexdump.hpp"


char* buffwrap(unsigned char value){
    static unsigned char buffer_value[4]= {0x00, 0x00, 0x00, 0x00};
    buffer_value[0] = value;
    return reinterpret_cast<char *>(buffer_value);
  
}
// char* buffcast(uint8_t num){
//     static char* bytePtr = reinterpret_cast<char*>(&num);
//     return bytePtr;
// }


namespace bpo = boost::program_options;


// Structure to hold board-specific configuration
struct BoardConfig {
	std::string ip;
    std::string prev_ip;
    std::array<unsigned char, 6> mac{{0x7C, 0xF0, 0x98, 0x01, 0x16, 0x00}};
    // CliSock sock;
    int boardIndex{-1};
    int chainPosition{-1};  // Position in daisy chain (0 = first/master, 1 = second, etc.)
    // std::map<unsigned int, unsigned char> registers;
};

// Ordered register configuration step
struct RegisterStep {
	unsigned int address;
	unsigned char value;
	std::string description;
};


class RoestiSampler : public fair::mq::Device
{
public:
    struct OptionKey {
        // static constexpr std::string_view DeviceIp	        {"ip"};
        static constexpr std::string_view BoardIP          {"board-ip"};
		static constexpr std::string_view ChainNum         {"chain-num"};
        
        static constexpr std::string_view DataPort	        {"port"};
        static constexpr std::string_view ControlPort       {"control-port"};
        static constexpr std::string_view OutputChannelName {"out-chan-name"};
        static constexpr std::string_view DQMChannelName    {"dqm-chan-name"};
        static constexpr std::string_view Timeout_ms        {"timeout"};
        // static constexpr std::string_view Mode              {"mode"};
        static constexpr std::string_view PollTimeout       {"poll-timeout"};
        // static constexpr std::string_view RBCP              {"rbcp"};
        // static constexpr std::string_view BoardIPs          {"board-ips"};
        static constexpr std::string_view DefaultRegisters  {"default-regs"};
        static constexpr std::string_view RedisHost         {"redis-host"};
		static constexpr std::string_view RedisPort         {"redis-port"};

        static constexpr std::string_view BoardRegisters         {"registers"};
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
    bool CheckRoestiHeader(char*);

private:

    void LoadConfigFromRedis();
    // void ResetBoard(BoardConfig& board, RBCP& rbcp);
    // void ConfigureBoard(BoardConfig& board, RBCP& rbcp);
    // bool ReadBoardData(BoardConfig& board, fair::mq::Parts& outParts);
    void ResetBoard(RBCP& rbcp);
    void ConfigureBoard(RBCP& rbcp);
    bool ReadBoardData(fair::mq::Parts& outParts);
    bool SingleBoardReadout();
   
    void LoadHardcodedConfig();

    uint64_t fNumIterations = 4;
    unsigned int fNumDestination = 0;
    unsigned int fPollTimeoutMS = 0;
    CliSock fSock;
    std::string fOutputChannelName;
    std::string fDQMChannelName;
    std::string fDeviceIp       {"000.000.000.000"};
    unsigned int fDataPort   = 24;
    unsigned int fControlPort = 4660;
    unsigned int fDeviceType  = 0x100;
    unsigned int fTimeout_ms = 0;
    unsigned int fMode = 0;
    uint64_t fMaxIterations = 10;
    KTimer fKt1;

    // std::vector<BoardConfig> fBoards;
    // std::map<unsigned int, unsigned char> fDefaultRegisters;
    // std::vector<RegisterStep> fRegisterSequence;
    
    std::string fRedisHost = "127.0.0.1";
    int fRedisPort = 6379;

    BoardConfig fBoard;
	std::vector<RegisterStep> fRegisterSequence;
    int fChainNum = 0;  // Which chain this board belongs to
    
};



void addCustomOptions(bpo::options_description& options)
{
    using opt = RoestiSampler::OptionKey;

    options.add_options()
        ("max-iterations",
            bpo::value<uint64_t>()->default_value(5),
            "Maximum number of iterations")

        (opt::OutputChannelName.data(),
            bpo::value<std::string>()->default_value("out"),
            "Output channel name")

        (opt::DQMChannelName.data(),
            bpo::value<std::string>()->default_value("dqm"),
            "DQM channel name")

        (opt::BoardIP.data(),
			bpo::value<std::string>()->required(),
			"IP address of the board this sampler will read from")

		(opt::ChainNum.data(),
			bpo::value<int>()->default_value(0),
			"Chain number this board belongs to (default: 0)")

        (opt::DataPort.data(),
            bpo::value<uint16_t>()->default_value(24),
            "Data port for all boards")
 
        (opt::ControlPort.data(),
            bpo::value<uint16_t>()->default_value(4660),
            "RBCP control port for all boards")

        (opt::Timeout_ms.data(),
            bpo::value<uint32_t>()->default_value(50000),
            "Timeout in milliseconds")

        // (opt::Mode.data(),
		// 	bpo::value<std::string>()->default_value("0x00"),
		// 	"Roesti run mode")

        (opt::PollTimeout.data(),
            bpo::value<uint32_t>()->default_value(1),
            "Poll timeout in milliseconds")

        (opt::DefaultRegisters.data(),
            bpo::value<std::vector<std::string>>()->multitoken(),
            "Default RBCP registers: addr val addr val ... (hex format: 0x110 0xf0)")
        
        (opt::RedisHost.data(),
            bpo::value<std::string>()->default_value("127.0.0.1"),
            "Redis server host")

        (opt::RedisPort.data(),
            bpo::value<int>()->default_value(6379),
            "Redis server port");
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


// void RoestiSampler::LoadHardcodedConfig()
// {
//     LOG(info) << "Loading hardcoded board configuration";
    
//     std::vector<std::string> defaultIPs = {"192.168.10.17", "192.168.10.18", "192.168.10.19"};
    
//     // for (size_t i = 0; i < defaultIPs.size(); ++i) {
//     //     BoardConfig board;
//     //     board.ip = defaultIPs[i];
//     //     board.boardIndex = i;
//     //     board.registers = fRegisterSequence; //board.registers = fRegisterSequence.begin(), fRegisterSequence.end());
        
        
//     //     // First board gets special daisy chain settings
//     //     if (i == 0) {
//     //         // board.registers[TRIG] = 0x02;
//     //         board.registers[DAISY_CHAIN] = 0x01;
//     //     }
        
//     //     fBoards.push_back(board);
//     // }
    
//     LOG(info) << "Loaded " << fBoards.size() << " boards from hardcoded defaults";
// }

// void RoestiSampler::StaticConfiguration(){
//  if (rbcp.Write(buffcast(0x00), 0x16a, 1) > 0) { LOG(info) << "-----SET: 0x16a"; }
// 	if (rbcp.Write(buffcast(0x00), 0x11d, 1) > 0) { LOG(info) << "-----SET: 0x11d"; }
// 	if (rbcp.Write(buffcast(0xf0), 0x110, 1) > 0) { LOG(info) << "-----SET: 0x110"; }
// 	if (rbcp.Write(buffcast(0x01), 0x112, 1) > 0) { LOG(info) << "-----SET: 0x112"; }
// }


void RoestiSampler::LoadConfigFromRedis()
{
    LOG(info) << "Connecting to Redis at " << fRedisHost << ":" << fRedisPort;
	
	struct timeval timeout = { 1, 500000 }; // 1.5 seconds
	redisContext *redis = redisConnectWithTimeout(fRedisHost.c_str(), fRedisPort, timeout);
	
	if (redis == nullptr || redis->err) {
		if (redis) {
			LOG(error) << "Redis connection error: " << redis->errstr;
			redisFree(redis);
		} else {
			LOG(error) << "Redis connection error: can't allocate redis context";
		}
		throw std::runtime_error("Failed to connect to Redis");
	}

	LOG(info) << "Successfully connected to Redis";

	// Select database 4
	redisReply *selectReply = (redisReply*)redisCommand(redis, "SELECT 0");
	if (selectReply == nullptr || selectReply->type == REDIS_REPLY_ERROR) {
		LOG(error) << "Failed to select Redis database 4";
		if (selectReply) freeReplyObject(selectReply);
		redisFree(redis);
		throw std::runtime_error("Failed to select Redis database");
	}
	freeReplyObject(selectReply);

	// Build the Redis key for this chain: roesti:chain_<num>:boards
	std::string chainKey = "roesti:chain_" + std::to_string(fChainNum) + ":boards";
	LOG(info) << "Loading board chain from Redis key: " << chainKey;

	// Get the complete board chain from Redis
	redisReply* reply = (redisReply*)redisCommand(redis, "LRANGE %s 0 -1", chainKey.c_str());
	if (!reply || reply->type != REDIS_REPLY_ARRAY || reply->elements == 0) {
		LOG(error) << "Failed to read board chain from Redis key: " << chainKey;
		if (reply) freeReplyObject(reply);
		redisFree(redis);
		throw std::runtime_error("No board chain found in Redis for chain " + std::to_string(fChainNum));
	}

	// Find this board's position in the chain
	bool found = false;
	int totalBoards = reply->elements;
	
	for (size_t i = 0; i < reply->elements; ++i) {
		std::string ip = reply->element[i]->str;
		
		if (ip == fBoard.ip) {
			found = true;
			fBoard.chainPosition = i;
			fBoard.boardIndex = i;
			
			// Set previous board IP if not first
			if (i > 0) {
				fBoard.prev_ip = reply->element[i - 1]->str;
				
				// Calculate MAC[5] from previous board's last IP octet
				int last_octet = std::stoi(fBoard.prev_ip.substr(fBoard.prev_ip.find_last_of('.') + 1));
				fBoard.mac[5] = static_cast<unsigned char>(last_octet + 128 - 16);
			}
			
			LOG(info) << "Board found in chain " << fChainNum << ":";
			LOG(info) << "  IP: " << fBoard.ip;
			LOG(info) << "  Position: " << fBoard.chainPosition << " of " << totalBoards;
			LOG(info) << "  Role: " << (i == 0 ? "MASTER (first)" : 
			                            (i == totalBoards - 1 ? "LAST" : "MIDDLE"));
			if (!fBoard.prev_ip.empty()) {
				LOG(info) << "  Previous board: " << fBoard.prev_ip;
				LOG(info) << "  MAC[5]: 0x" << std::hex << static_cast<int>(fBoard.mac[5]) << std::dec;
			}
			
			break;
		}
	}

	freeReplyObject(reply);
	redisFree(redis);

	if (!found) {
		LOG(error) << "Board IP " << fBoard.ip << " not found in Redis chain " << fChainNum;
		throw std::runtime_error("Board IP not found in chain configuration");
	}

}



void RoestiSampler::ResetBoard(RBCP& rbcp)
{
    // LOG(info) << "Board " << board.boardIndex << " - Performing reset sequence";

    // if (!rbcp.Open(board.ip.c_str(), fControlPort)) {
    //     LOG(error) << "Failed to RESET RBCP connection to board " << board.boardIndex;
    //     return;
    // }

    LOG(info) << "Performing reset sequence for board at " << fBoard.ip;
    
    if (!rbcp.Open(fBoard.ip.c_str(), fControlPort)) {
		LOG(error) << "Failed to open RBCP connection for reset";
		throw std::runtime_error("RBCP connection failed");
	}

    rbcp.Write(buffwrap(0x00), 0x16a, 1);
    rbcp.Write(buffwrap(0x00), 0x11d, 1);
    rbcp.Write(buffwrap(0xf0), 0x110, 1);
    rbcp.Write(buffwrap(0x01), 0x112, 1);

    // LOG(info) << "RESET: " << board.ip << " - DONE.";
    
    rbcp.Close();
    LOG(info) << "Reset sequence complete";
}



void RoestiSampler::ConfigureBoard(RBCP& rbcp)
{
    LOG(info) << "Configuring board at " << fBoard.ip << " (position " << fBoard.chainPosition << ")";

	if (!rbcp.Open(fBoard.ip.c_str(), fControlPort)) {
		LOG(error) << "Failed to open RBCP connection";
		throw std::runtime_error("RBCP connection failed");
	}

	LOG(info) << "Opened RBCP connection to " << fBoard.ip << " Port " << fControlPort;

	// Get total number of boards in chain from the position
	// (We know our position, so we can determine if we're last)
	// This is set during LoadBoardConfigFromRedis based on chain length
	bool isLastBoard = false;  // Will be set based on Redis data or can query again
	
	// Apply all register configurations in exact order from fRegisterSequence
	for (const auto& step : fRegisterSequence) {
		unsigned char val = step.value;

		// Override DAISY_CHAIN value based on board position
		if (step.address == DAISY_CHAIN) {
			// Last board gets 0x00, all others get 0x01
			// For now, check if prev_ip exists (if board 0, no prev; if last, has prev but is still last)
			// We need to know total chain length - store during Redis load
			val = isLastBoard ? 0x00 : 0x01;
		}

		if (rbcp.Write(buffwrap(val), step.address, 1) > 0) {
			LOG(debug) << "SET: " << step.description << " (0x" << std::hex 
			           << step.address << ") = 0x" << static_cast<int>(val) << std::dec;
		} else {
			LOG(error) << "FAILED: " << step.description << " (0x" 
			           << std::hex << step.address << ")" << std::dec;
		}
	}

	// If not the first board, configure previous board's IP and MAC
	if (!fBoard.prev_ip.empty()) {
		LOG(info) << "Configuring previous board IP: " << fBoard.prev_ip;

		unsigned char ipb[4];
		sscanf(fBoard.prev_ip.c_str(), "%hhu.%hhu.%hhu.%hhu", 
		       &ipb[0], &ipb[1], &ipb[2], &ipb[3]);

		// Write IP address (reverse order: 0x163, 0x162, 0x161, 0x160)
		rbcp.Write(buffwrap(ipb[0]), 0x163, 1);
		rbcp.Write(buffwrap(ipb[1]), 0x162, 1);
		rbcp.Write(buffwrap(ipb[2]), 0x161, 1);
		rbcp.Write(buffwrap(ipb[3]), 0x160, 1);

		// Write MAC address (6 bytes starting at 0x164)
		for (size_t i = 0; i < fBoard.mac.size(); ++i) {
			rbcp.Write(buffwrap(fBoard.mac[i]), 0x164 + i, 1);
		}

		// Enable TCP
		unsigned char one = 0x01;
		rbcp.Write(buffwrap(one), 0x16a, 1);
		rbcp.Write(buffwrap(one), 0x16d, 1);
		
		LOG(info) << "Previous board IP/MAC configuration complete";
	}

	rbcp.Close();
	LOG(info) << "Board configuration complete";
}



void RoestiSampler::InitTask() {
  LOG(debug) << __FUNCTION__;

//   try {

    PrintConfig(fConfig, "channel-config", __PRETTY_FUNCTION__);
    PrintConfig(fConfig, "chans.", __PRETTY_FUNCTION__);

    using opt = OptionKey;

    fBoard.ip = fConfig->GetValue<std::string>(opt::BoardIP.data());
	fChainNum = fConfig->GetValue<int>(opt::ChainNum.data());

    fOutputChannelName = fConfig->GetValue<std::string>(opt::OutputChannelName.data());
	fDQMChannelName = fConfig->GetValue<std::string>(opt::DQMChannelName.data());
	fDataPort = fConfig->GetValue<uint16_t>(opt::DataPort.data());
	fControlPort = fConfig->GetValue<uint16_t>(opt::ControlPort.data());
	fTimeout_ms = fConfig->GetValue<uint32_t>(opt::Timeout_ms.data());
	fPollTimeoutMS = fConfig->GetValue<uint32_t>(opt::PollTimeout.data());
	fRedisHost = fConfig->GetValue<std::string>(opt::RedisHost.data());
	fRedisPort = fConfig->GetValue<int>(opt::RedisPort.data());
		
	fNumDestination = GetNumSubChannels(fOutputChannelName);

    // LOG(info) << "Configuration loaded:";
    // LOG(info) << "  Output channel: " << fOutputChannelName;
    // LOG(info) << "  Data port: " << fDataPort;
    // LOG(info) << "  Control port: " << fControlPort;
    // LOG(info) << "  Timeout: " << fTimeout_ms << " ms";
    // LOG(info) << "  Redis: " << fRedisHost << ":" << fRedisPort;
    // LOG(info) << "  Number of destinations: " << fNumDestination;

    fRegisterSequence = {
        {RESET_INTR,          0xf0, "RESET_INTR"},
		{DRS4_SAMPLING_SPEED, 30,   "DRS4_SAMPLING_SPEED"},
		{ADC_RESET_SYNC,      0x08, "ADC_RESET_SYNC"},
		{ADC_OUT_SYNC,        0x06, "ADC_OUT_SYNC"},
		{ADC_RESET_CH07,      0x3C, "ADC_RESET_CH07"},
		{ADC_RESET_CH815,     0x3C, "ADC_RESET_CH815"},
        {ADC_PWM_CH07,        0x03, "ADC_PWM_CH07"},
		{ADC_PWM_CH07,        0x00, "ADC_PWM_CH07_2"},
        {ADC_PWM_CH815,       0x03, "ADC_PWM_CH815"},
		{ADC_PWM_CH815,       0x00, "ADC_PWM_CH815_2"},
		{ADC_OUT_CH07,        0x00, "ADC_OUT_CH07"},
		{ADC_OUT_CH815,       0x00, "ADC_OUT_CH815"},
		{ADC_TEST_CH07,       0x00, "ADC_TEST_CH07"},
		{ADC_TEST_CH815,      0x00, "ADC_TEST_CH815"},
		{THR_UPP,             0x08, "THR_UPP"},
		{THR_LOW,             0x00, "THR_LOW"},
		{TRIG,                0x04, "TRIG"},               
		{ASD_CHECK_START_TIME,180, "ASD_CHECK_START_TIME"},
		{ASD_WAIT_WIDTH,      210, "ASD_WAIT_WIDTH"},
		{ZSUP,                0x00, "ZSUP"},
		{DSEND,               0x00, "DSEND"},
		{DAC_SYNC,            0x01, "DAC_SYNC"},
		{RESET_DRS,           0xf0, "RESET_DRS"},
		{RESET_INTR,          0xf0, "RESET_INTR_2"},
		{DATA_LAT,            0x2F, "DATA_LAT"},
		{DATA_RESET,          0x01, "DATA_RESET"},
		{ADC_OUT_PHASE_CH07,  0x00, "ADC_OUT_PHASE_CH07"},
		{ADC_OUT_PHASE_CH815, 0x00, "ADC_OUT_PHASE_CH815"},
		{DAISY_CHAIN,         0x01, "DAISY_CHAIN"}       // Overridden per board
		// {SiTCP_Negation,      0x00, "SiTCP_Negation"}
	};



    // Load configuration from redis
    LoadConfigFromRedis();

    // LOG(info) << "Configuration set for " << fBoards.size() << " boards";
    // LOG(info) << "Number of destinations: " << fNumDestination;

    
    RBCP rbcp;
  
    // ---Configure all boards
    // for (auto& board : fBoards){
    //     ResetBoard(board, rbcp);
    // }

    // for (auto it = fBoards.rbegin();it != fBoards.rend(); ++it){
    //     ConfigureBoard(*it, rbcp);
    // }

    // rbcp.Close();

    // Validate that board was found and configured
	if (fBoard.boardIndex < 0) {
		LOG(error) << "Board IP " << fBoard.ip << " not found in Redis chain " << fChainNum;
		LOG(error) << "Cannot proceed with initialization";
		throw std::runtime_error("Board not configured - not found in Redis");
	}

    // RESET PHASE
	LOG(info) << "\n========== RESET PHASE ==========";
	ResetBoard(rbcp);

	
	// Wait after reset
	LOG(info) << "Waiting 1 second after reset...";
	std::this_thread::sleep_for(std::chrono::seconds(1));
	
	// SETUP PHASE
	LOG(info) << "\n========== SETUP PHASE ==========";
	ConfigureBoard(rbcp);

    
    std::this_thread::sleep_for(std::chrono::seconds(1));
    // rbcp.Close();

    LOG(info) << "\n========== INITIALIZATION COMPLETE ==========\n";

}

bool CheckRoestiHeader(char *buf) { return true; }

int CheckRoestiDataPacket(unsigned char *data, int len) {

    unsigned char *buf = data;

    while (true) {
      struct Roesti::DataPacketHeader *ppheader =
          reinterpret_cast<Roesti::DataPacketHeader *>(buf);
      if (ppheader->data_packet_word != Roesti::DATA_PACKET_MAGIC)
        break;

      #ifdef DEBUG
        std::cout << "DP MAGIC: 0x" << std::hex << ppheader->data_packet_word;
        std::cout << ", res: 0x" << std::hex
                  << static_cast<unsigned int>(ppheader->reserved_header);
        std::cout << ", ch: " << std::dec
                  << static_cast<unsigned int>(ppheader->channel_num);
        std::cout << ", stop: " << std::dec << ppheader->stop_num;
        std::cout << ", Len: " << std::dec << ppheader->data_length;
        std::cout << std::endl;
      #endif

      struct Roesti::DataPacketFooter *ppfooter =
          reinterpret_cast<Roesti::DataPacketFooter *>(
              buf + ppheader->data_length + sizeof(Roesti::DataPacketHeader));

      #ifdef DEBUG
        std::cout << "DP footer: 0x" << std::hex << ppfooter->footer;
        std::cout << ", keyword: 0x" << static_cast<unsigned int>(ppfooter->keyword);
        std::cout << ", res: 0x" << static_cast<unsigned int>(ppfooter->reserved);
        std::cout << std::dec << std::endl;
      #endif

      buf += sizeof(Roesti::DataPacketHeader) + ppheader->data_length +
             sizeof(Roesti::DataPacketFooter);
      if ((buf - data) > len)
        break;
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

// single board
bool RoestiSampler::SingleBoardReadout(){
    fair::mq::Parts outParts;

    int nflag;

    //---Read header

    unsigned char rec_buf[40*1000];
    struct Roesti::RoestiHeader *pheader = reinterpret_cast<struct Roesti::RoestiHeader *>(rec_buf);
    int header_size = sizeof(struct Roesti::RoestiHeader);

    int header_read = fSock.Receive(reinterpret_cast<char *>(pheader), sizeof(struct Roesti::RoestiHeader) , nflag);

    
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
    // LOG(debug4) << "SentEventNumber# " << (pheader->event_num & 0xff) << std::endl;

    //---Read data packet
    unsigned char *TCP_data = rec_buf + sizeof(struct Roesti::RoestiHeader);
    
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

    #ifdef DEBUG
        // std::cout << "-------STF HEADER VALUES--------"<< std::endl;
        // std::cout << "Magic:" << std::hex << pstfheader->magic << std::endl;
        // std::cout << "Event:" << std::dec << pstfheader->timeFrameId << std::endl;
        // std::cout << "IP" << std::dec << pstfheader->FEMId << std::endl;
    #endif

    //--roesti header+ datapacket exact
    unsigned int fullPacketSize = sizeof(struct Roesti::RoestiHeader) + 
                                  sizeof(Roesti::DataPacketHeader) +
                                  pheader->length + 
                                  sizeof(Roesti::DataPacketFooter);
    outParts.AddPart(NewMessage(fullPacketSize));

    // outParts.AddPart(NewMessage(sizeof(rec_buf)));
    auto &msgRoesti = outParts[1];

    char *msgbuf = reinterpret_cast<char *>(msgRoesti.GetData());
    // memcpy(msgbuf, rec_buf, sizeof(rec_buf));
    memcpy(msgbuf, pheader, sizeof(struct Roesti::RoestiHeader));
    msgbuf+=sizeof(struct Roesti::RoestiHeader);
    memcpy(msgbuf, TCP_data, dp_len);


    // Send all outParts
    Send(outParts, fOutputChannelName);
    #ifdef DEBUG
        LOG(warn) << "PACKET sent";
    #endif

    return true;
}

bool RoestiSampler::ReadBoardData(fair::mq::Parts& outParts)
{
    int nflag;
	unsigned char rec_buf[40*1000];
	
	struct Roesti::RoestiHeader *pheader = reinterpret_cast<struct Roesti::RoestiHeader *>(rec_buf);
	int header_size = sizeof(struct Roesti::RoestiHeader);

    // -- check empty header
    if (header_size == 0) {
        LOG(warn) << "Zero-length header";
        return false;
    }
    

    int header_read = fSock.Receive(reinterpret_cast<char *>(pheader), sizeof(struct Roesti::RoestiHeader) , nflag);


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

	//---Read data packet
    unsigned char *TCP_data = rec_buf + sizeof(struct Roesti::RoestiHeader);
    
    unsigned int dp_read = 0;
    unsigned int dp_len = pheader->length;

    // -- check empty packet
    if (dp_len == 0) {
        LOG(warn) << "Zero-length packet";
        return false;
    }

        dp_read += fSock.Receive(reinterpret_cast<char *>(TCP_data), dp_len - dp_read, nflag);
        
        
        #ifdef DEBUG
            std::cout<< "--------------------DATA--------------------" << std::endl;
            std::cout << "data packet read/length: " << dp_read << "/" << dp_len << std::endl;
        #endif
        
        // if (dp_read >= dp_len) {
        // 	LOG(warn) << "Length check: " << dp_read << "/" << dp_len;
        // }else
        
        
        // if (dp_read < dp_len){
        //     LOG(error) << "Insufficent size of data";
          if (dp_read <= 0 || dp_read < dp_len) {
            LOG(warn) << "Incomplete/empty packet: " << dp_read << "/" << dp_len;
            return false;
        }else{
            if (nflag == EAGAIN) {
                LOG(warn) << "Timeout and Retry";
                dp_read += fSock.Receive(reinterpret_cast<char *>(TCP_data), dp_len - dp_read, nflag);
            }
        }


    //--- Unpack data packet
    CheckRoestiDataPacket(TCP_data, dp_read);

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
    
    pstfheader->chain_num = 0;
    pstfheader->board_num = 0;


    //--roesti header+ datapacket exact
    unsigned int fullPacketSize = sizeof(struct Roesti::RoestiHeader) + 
                                  sizeof(Roesti::DataPacketHeader) +
                                  pheader->length + 
                                  sizeof(Roesti::DataPacketFooter);
    outParts.AddPart(NewMessage(fullPacketSize));



    // outParts.AddPart(NewMessage(sizeof(rec_buf)));
    auto &msgRoesti = outParts[1];

    char *msgbuf = reinterpret_cast<char *>(msgRoesti.GetData());
    // memcpy(msgbuf, rec_buf, sizeof(rec_buf));
    memcpy(msgbuf, pheader, sizeof(struct Roesti::RoestiHeader));
    msgbuf+=sizeof(struct Roesti::RoestiHeader);
    memcpy(msgbuf, TCP_data, dp_len);


    return true;
}

bool RoestiSampler::ConditionalRun()
{

    //--single
    // SingleBoardReadout();

    //--multi
    // for (auto& board : fBoards) {
	// 	fair::mq::Parts outParts;  // Fresh parts for each board
		
	// 	if (!ReadBoardData(board, outParts)) {
	// 		LOG(error) << "Failed to read data from board " << board.boardIndex;
	// 		continue;  // Skip this board
	// 	}

	// 	if (outParts.Size() > 0) {
	// 		Send(outParts, fOutputChannelName);
	// 		// #ifdef DEBUG
	// 			LOG(debug) << "Sent data from board " << board.ip;
	// 		// #endif
	// 	}
	// }

    // return true;

    //--single new
    fair::mq::Parts outParts;
	
	if (!ReadBoardData(outParts)) {
		LOG(error) << "Failed to read data from board";
		
	}
    if (outParts.Size() > 0) {
		Send(outParts, fOutputChannelName);
	}
	

    // ---read and send in one function
    // SingleBoardReadout();

	return true;

    
}


void RoestiSampler::PostRun()
{
// 	LOG(debug) << __FUNCTION__;
// 	fNumIterations = 0;
    // fSock.Close();

    // LOG(debug) << __FUNCTION__;
    // for (auto& board : fBoards) {
    //     board.sock.Close();
    // }

    LOG(debug) << __FUNCTION__;
	fSock.Close();
}


//bool RoestiSampler::PreRun()
void RoestiSampler::PreRun()
{
    // --single
    // // LOG(debug) << __FUNCTION__;
    // if (fTimeout_ms > 0) fSock.SetTimeOut_ms(fTimeout_ms);
    // // fSock.SetBuffer(33554432);
    // int sock = fSock.Connect(fDeviceIp.c_str(), fDataPort);
    
    // if (sock < 0) {
    //         LOG(error) << "Connection error for board " << board.boardIndex 
    // } else {
    //         LOG(info) << "Connected to board " << board.boardIndex 
    //                   << " at " << board.ip << ":" << fDataPort;
    // }
    // return;

    //--multiboard
    // LOG(debug) << __FUNCTION__;
    
    // for (auto& board : fBoards) {
    //     if (fTimeout_ms > 0) {
    //         board.sock.SetTimeOut_ms(fTimeout_ms);
    //     }
        
    //     int sock = board.sock.Connect(board.ip.c_str(), fDataPort);
    //     if (sock < 0) {
    //         LOG(error) << "Connection error for board " << board.boardIndex 
    //                    << " at " << board.ip << ":" << fDataPort;
    //     } else {
    //         LOG(info) << "Connected to board " << board.boardIndex 
    //                   << " at " << board.ip << ":" << fDataPort;
    //     }
    // }

    LOG(debug) << __FUNCTION__;
	
	if (fTimeout_ms > 0) {
		fSock.SetTimeOut_ms(fTimeout_ms);
	}
	
	int sock = fSock.Connect(fBoard.ip.c_str(), fDataPort);
	if (sock < 0) {
		LOG(error) << "Connection error at " << fBoard.ip << ":" << fDataPort;
		throw std::runtime_error("Failed to connect to board");
	} else {
		LOG(info) << "Connected to board at " << fBoard.ip << ":" << fDataPort;
	}
    

}


void RoestiSampler::Run()
{
    LOG(debug) << __FUNCTION__;
}

