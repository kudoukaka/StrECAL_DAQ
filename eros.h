#ifndef INC_EROS
#define INC_EROS
#include <cstdint>
#endif

namespace Eros {

constexpr uint32_t MAGIC {0x89ABCDEF};

#pragma pack(2)
struct ErosHeader{

	uint32_t magic       ; //{MAGIC};
	uint16_t type        ; //{0};
	uint16_t ipaddr      ; //{0};
	uint32_t length      ; //{0};
	uint32_t event_num   ; //{0};
	uint16_t seu_count   ; //{0};
	uint16_t mbu_count   ; //{0};
	uint16_t seu_inf_size; //{0};
	uint16_t reserved    ;
};

struct ErosFooter {
	uint32_t footer;
	uint32_t crc32;
};

#pragma pack(1)
constexpr uint32_t DATA_PACKET_MAGIC {0x01234};
struct DataPacketHeader {
	uint16_t data_packet_word;
	uint8_t reserved_header;
	uint8_t channel_num;
	uint16_t stop_num;
	uint16_t data_length;
};

struct DataPacketFooter {
	uint16_t footer;
	uint8_t keyword;
	uint8_t reserved;
};

struct DataPacketData{
	uint16_t data;
};

struct ErosDataBody{
	uint16_t data_packet_word;
	uint8_t reserved_header;
	uint8_t channel_num;
	uint16_t stop_num;
	uint16_t data_length;
	uint16_t data;
	uint16_t data_footer;
	uint8_t keyword;
	uint8_t reserved_footer;
};

#pragma pack()

}



//network setting
#define SERVPORT 24
#define UDP_port 4660

//Register map

constexpr unsigned int  DRS4_SAMPLING_SPEED = 0x170     ;  
constexpr unsigned int  ADC_RESET_SYNC      = 0x00030000;    // ADC Reset for Synchronous
constexpr unsigned int  ADC_OUT_SYNC        = 0x00030002;    // ADC Output Mode for Synchronous
constexpr unsigned int  ADC_RESET_CH07      = 0x00010000;    // ADC Reset for ch0-ch7 Signals
constexpr unsigned int  ADC_RESET_CH815     = 0x00020000;    // ADC Reset for ch8-ch15 Signals
constexpr unsigned int  ADC_PWM_CH07        = 0x00010008;    // ADC Power Model for ch0-ch7 Signals
constexpr unsigned int  ADC_PWM_CH815       = 0x00020008;    // ADC Power Model for ch8-ch15 Signals
constexpr unsigned int  ADC_OUT_CH07        = 0x00010014;    // ADC Output Mode for ch0-ch7 Signals
constexpr unsigned int  ADC_OUT_CH815       = 0x00020014;    // ADC Output Mode for ch8-ch15 Signals
constexpr unsigned int  ADC_TEST_CH07       = 0x0001000D;    // ADC Test Mode for ch0-ch7 Signals
constexpr unsigned int  ADC_TEST_CH815      = 0x0002000D;    // ADC Test Mode for ch8-ch15 signals
constexpr unsigned int  ADC_OUT_PHASE_CH07  = 0x00010016;    // ADC Output Phase for ch0-ch7 Signals
constexpr unsigned int  ADC_OUT_PHASE_CH815 = 0x00020016;    // ADC Output Phase for ch8-ch15 Signals



constexpr unsigned int  THR_UPP     = 0x115;    // Threshold upper
constexpr unsigned int  THR_LOW     = 0x116;    // Threshold lower
constexpr unsigned int  TRIG        = 0x11d;    // Trigger send / Read mode
constexpr unsigned int  ASD_CHECK_START_TIME = 0x141;    // -
constexpr unsigned int  ASD_WAIT_WIDTH = 0x142;          // -
constexpr unsigned int  ZSUP        = 0x140;    // Zero suppression
constexpr unsigned int  DSEND       = 0x171;    // Decision on trigger
constexpr unsigned int  DAC_SYNC    = 0x119;    // DAC Sync
constexpr unsigned int  RESET_DRS   = 0x111;    // DSR reset
constexpr unsigned int  RESET_INTR  = 0x110;    // FPGA internal reset
constexpr unsigned int  DATA_LAT    = 0x11A;    // ADC data latency
constexpr unsigned int  DATA_RESET  = 0x112;    // emties ADC data
constexpr unsigned int  DAISY_CHAIN = 0x153;    // Daisy chain selection, value 0 -farthest 1-otherwise
constexpr unsigned int  SiTCP_Negation = 0x10;   
