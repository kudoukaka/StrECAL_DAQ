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
#include "eros.h"

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


class ErosSampler : public fair::mq::Device
{
public:
    struct OptionKey {
        static constexpr std::string_view BoardIP          {"board-ip"};
		static constexpr std::string_view ChainNum         {"chain-num"};
        static constexpr std::string_view DataPort	        {"port"};
        static constexpr std::string_view ControlPort       {"control-port"};
        static constexpr std::string_view OutputChannelName {"out-chan-name"};
        static constexpr std::string_view DQMChannelName    {"dqm-chan-name"};
        static constexpr std::string_view Timeout_ms        {"timeout"};
        static constexpr std::string_view PollTimeout       {"poll-timeout"};
        static constexpr std::string_view DefaultRegisters  {"default-regs"};
        static constexpr std::string_view RedisHost         {"redis-host"};
		static constexpr std::string_view RedisPort         {"redis-port"};
        static constexpr std::string_view BoardRegisters    {"registers"};
        
    };

    ErosSampler() : fair::mq::Device() {};
    ErosSampler(const ErosSampler&)	    = delete;
    ErosSampler& operator=(const ErosSampler&) = delete;
    ~ErosSampler() = default;

protected:
    void Init() override;
    void InitTask() override;
    void PreRun() override;
    bool ConditionalRun() override;
    void Run() override;
    void PostRun() override;
    bool CheckErosHeader(char*);

private:

    void LoadConfigFromRedis();
    void ResetBoard(RBCP& rbcp);
    void ConfigureBoard(RBCP& rbcp);
    bool ReadBoardData(fair::mq::Parts& outParts);

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

    std::string fRedisHost = "127.0.0.1";
    int fRedisPort = 6379;;

    BoardConfig fBoard;
	std::vector<RegisterStep> fRegisterSequence;
    int fChainNum = 0;  // Which chain this board belongs to

    std::unordered_map<std::string, std::string> fBoardRegisterOverrides;
    
};



void addCustomOptions(bpo::options_description& options)
{
    using opt = ErosSampler::OptionKey;

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
    return std::make_unique<ErosSampler>();
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


void ErosSampler::Init()
{
    LOG(debug) << __FUNCTION__;
    
}


void ErosSampler::LoadConfigFromRedis()
{
    LOG(info) << "Connecting to Redis at " << fRedisHost << ":" << fRedisPort;
	
    // TODO: Use options plugin to read the value from redis

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

	// Select database 0
	redisReply *selectReply = (redisReply*)redisCommand(redis, "SELECT 0");
	if (selectReply == nullptr || selectReply->type == REDIS_REPLY_ERROR) {
		LOG(error) << "Failed to select Redis database 4";
		if (selectReply) freeReplyObject(selectReply);
		redisFree(redis);
		throw std::runtime_error("Failed to select Redis database");
	}
	freeReplyObject(selectReply);

	// Build the Redis key for this chain: eros:chain_<num>:boards
	std::string chainKey = "eros:chain_" + std::to_string(fChainNum) + ":boards";
	LOG(info) << "Loading board chain from Redis key: " << chainKey;

	// Get the complete board chain from Redis
	redisReply* reply = (redisReply*)redisCommand(redis, "LRANGE %s 0 -1", chainKey.c_str());
	if (!reply || reply->type != REDIS_REPLY_ARRAY || reply->elements == 0) {
		LOG(error) << "Failed to read board chain from Redis key: " << chainKey;
		if (reply) freeReplyObject(reply);
		redisFree(redis);
		throw std::runtime_error("No board chain found in Redis for chain " + std::to_string(fChainNum));
	}

    //--IP and MAC calculation
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

    std::string boardKey = "eros:chain:" + std::to_string(fChainNum) + ":board:" + std::to_string(fBoard.boardIndex);
    LOG(info) << "Loading board register config from Redis key: " << boardKey;

    fBoardRegisterOverrides.clear();
    redisReply* boardReply = (redisReply*)redisCommand(redis, "HGETALL %s", boardKey.c_str());

    if (boardReply == nullptr) {
        LOG(warn) << "Redis command failed (HGETALL " << boardKey << "): " << redis->errstr;
    } else if (boardReply->type == REDIS_REPLY_ARRAY) {
        for (size_t i = 0; i + 1 < boardReply->elements; i += 2) {
            if (boardReply->element[i]->type == REDIS_REPLY_STRING &&
                boardReply->element[i + 1]->type == REDIS_REPLY_STRING) {
                std::string field(boardReply->element[i]->str, boardReply->element[i]->len);
                std::string value(boardReply->element[i + 1]->str, boardReply->element[i + 1]->len);
                fBoardRegisterOverrides.emplace(std::move(field), std::move(value));
            }
        }
        LOG(info) << "Loaded " << fBoardRegisterOverrides.size()
                  << " fields from " << boardKey;
    } else {
        LOG(warn) << "No board config hash found at key: " << boardKey;
    }
    if (boardReply) freeReplyObject(boardReply);

    redisFree(redis);

}



void ErosSampler::ResetBoard(RBCP& rbcp)
{

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



void ErosSampler::ConfigureBoard(RBCP& rbcp)
{
    LOG(info) << "Configuring board at " << fBoard.ip << " (position " << fBoard.chainPosition << ")";

	if (!rbcp.Open(fBoard.ip.c_str(), fControlPort)) {
		LOG(error) << "Failed to open RBCP connection";
		throw std::runtime_error("RBCP connection failed");
	}

	LOG(info) << "Opened RBCP connection to " << fBoard.ip << " Port " << fControlPort;

	bool isLastBoard = false;  // Will be set based on Redis data or can query again
	
	
	for (const auto& step : fRegisterSequence) {
		unsigned char val = step.value;

		// Override DAISY_CHAIN value based on board position
		if (step.address == DAISY_CHAIN) {
			// Last board gets 0x00, all others get 0x01
			val = isLastBoard ? 0x00 : 0x01;
		}

        //Auto map config registers
        if (auto it = fBoardRegisterOverrides.find(step.description); it != fBoardRegisterOverrides.end()) {
            val = std::stoi(it->second);
            LOG(debug) << "REDIS OVERRIDE: " << step.description << " = 0x"
                       << std::hex << static_cast<int>(val) << std::dec;
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



void ErosSampler::InitTask() {
    LOG(debug) << __FUNCTION__;


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
		{TRIG,                0x02, "TRIG"},         
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
	};



    // Load configuration from redis
    LoadConfigFromRedis();
    
    //rbcp strart
    RBCP rbcp;
  
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

bool CheckErosHeader(char *buf) { return true; }

int CheckErosDataPacket(unsigned char *data, int len) {

    unsigned char *buf = data;

    while (true) {
      struct Eros::DataPacketHeader *ppheader =
          reinterpret_cast<Eros::DataPacketHeader *>(buf);
      if (ppheader->data_packet_word != Eros::DATA_PACKET_MAGIC)
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

      struct Eros::DataPacketFooter *ppfooter =
          reinterpret_cast<Eros::DataPacketFooter *>(
              buf + ppheader->data_length + sizeof(Eros::DataPacketHeader));

      #ifdef DEBUG
        std::cout << "DP footer: 0x" << std::hex << ppfooter->footer;
        std::cout << ", keyword: 0x" << static_cast<unsigned int>(ppfooter->keyword);
        std::cout << ", res: 0x" << static_cast<unsigned int>(ppfooter->reserved);
        std::cout << std::dec << std::endl;
      #endif

      buf += sizeof(Eros::DataPacketHeader) + ppheader->data_length +
             sizeof(Eros::DataPacketFooter);
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


bool ErosSampler::ReadBoardData(fair::mq::Parts& outParts)
{
    int nflag;
	unsigned char rec_buf[40*1000];
	
	struct Eros::ErosHeader *pheader = reinterpret_cast<struct Eros::ErosHeader *>(rec_buf);
	int header_size = sizeof(struct Eros::ErosHeader);

    // -- check empty header
    if (header_size == 0) {
        LOG(warn) << "Zero-length header";
        return false;
    }
    

    int header_read = fSock.Receive(reinterpret_cast<char *>(pheader), sizeof(struct Eros::ErosHeader) , nflag);


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

	//read data packet
    unsigned char *TCP_data = rec_buf + sizeof(struct Eros::ErosHeader);
    
    unsigned int dp_read = 0;
    unsigned int dp_len = pheader->length;

    //check empty packet
    if (dp_len == 0) {
        LOG(warn) << "Zero-length packet";
        return false;
    }

    dp_read += fSock.Receive(reinterpret_cast<char *>(TCP_data), dp_len - dp_read, nflag);

    #ifdef DEBUG
        std::cout<< "--------------------DATA--------------------" << std::endl;
        std::cout << "data packet read/length: " << dp_read << "/" << dp_len << std::endl;
    #endif
     

    if (dp_read <= 0 || dp_read < dp_len) {
        LOG(warn) << "Incomplete/empty packet: " << dp_read << "/" << dp_len;
        return false;
    }else{
        if (nflag == EAGAIN) {
            LOG(warn) << "Timeout and Retry";
            dp_read += fSock.Receive(reinterpret_cast<char *>(TCP_data), dp_len - dp_read, nflag);
        }
    }

    //Unpack data packet
    CheckErosDataPacket(TCP_data, dp_read);

    //Read footer

    int footer_size = 8;
    char recvbuff[16];
    struct Eros::ErosFooter *pfooter = reinterpret_cast<Eros::ErosFooter *>(recvbuff);
    int footer_read = fSock.Receive(reinterpret_cast<char *>(&recvbuff), footer_size, nflag);

    #ifdef DEBUG
        std::cout<< "--------------------FOOTER--------------------" << std::endl;
        std::cout << "Footer: size:" << std::dec << footer_read
            << ", key: 0x" << std::hex << pfooter->footer
            << ", crc: 0x" << std::hex << pfooter->crc32 << std::endl;
        std::cout << std::dec;
    #endif

    outParts.AddPart(NewMessage(sizeof(SubTimeFrame::Header)));
    auto &msgSTFHeader = outParts[0];

    struct SubTimeFrame::Header *pstfheader = reinterpret_cast<struct SubTimeFrame::Header *>(msgSTFHeader.GetData());

    pstfheader->magic = SubTimeFrame::Magic;
    pstfheader->timeFrameId = pheader->event_num;
    pstfheader->FEMType = static_cast<uint32_t>(pheader->type) & 0xff;
    pstfheader->FEMId = static_cast<uint32_t>(pheader->ipaddr) & 0xff;

    pstfheader->length =  pheader->length;
    pstfheader->numMessages = 1;
    struct timeval now;
    gettimeofday(&now, nullptr);
    pstfheader->time_sec = now.tv_sec;
    pstfheader->time_usec = now.tv_usec;
    
    pstfheader->chain_num = 0;
    pstfheader->board_num = 0;


    //eros header+ datapacket exact
    unsigned int fullPacketSize = sizeof(struct Eros::ErosHeader) + 
                                  sizeof(Eros::DataPacketHeader) +
                                  pheader->length + 
                                  sizeof(Eros::DataPacketFooter);
    outParts.AddPart(NewMessage(fullPacketSize));



    auto &msgEros = outParts[1];

    char *msgbuf = reinterpret_cast<char *>(msgEros.GetData());
    memcpy(msgbuf, pheader, sizeof(struct Eros::ErosHeader));
    msgbuf+=sizeof(struct Eros::ErosHeader);
    memcpy(msgbuf, TCP_data, dp_len);


    return true;
}

bool ErosSampler::ConditionalRun()
{

    fair::mq::Parts outParts;
	
	if (!ReadBoardData(outParts)) {
		LOG(error) << "Failed to read data from board";
		
	}
    if (outParts.Size() > 0) {
		Send(outParts, fOutputChannelName);
	}
	

	return true;

    
}


void ErosSampler::PostRun()
{

    LOG(debug) << __FUNCTION__;
	fSock.Close();
}


//bool ErosSampler::PreRun()
void ErosSampler::PreRun()
{
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


void ErosSampler::Run()
{
    LOG(debug) << __FUNCTION__;
}

