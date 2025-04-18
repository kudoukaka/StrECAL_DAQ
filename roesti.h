#ifndef INC_ROESTI
#define INC_ROESTI
#endif

namespace Roesti {
// network byte order
struct Header {
	unsigned char  type;
	unsigned char  id;
	unsigned short sent_num;
	unsigned short time;
	unsigned short len;
	unsigned int   trig_count;
};

struct RoestyHeader{
	unsigned char magicword;
	unsigned long length;
	unsigned int event_num;
};

}

// constexpr int T_RAW	         = 0x01;
// constexpr int T_SUPPRESS     = 0x02;
// constexpr int T_BOTH         = 0x03;
// constexpr int T_RAW_OLD	     = 0x22;
// constexpr int T_SUPPRESS_OLD = 0x20;

// constexpr int N_CH = 32;


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



constexpr unsigned int  THR_UP      = 0x115;    // Threshold up ?
constexpr unsigned int  THR_DOWN    = 0x116;    // Threshold down ?
constexpr unsigned int  TRIG        = 0x11d;    // Trigger send / Read mode
constexpr unsigned int  ASD_CHECK_START_TIME = 0x141;    // -
constexpr unsigned int  ASD_WAIT_WIDTH = 0x142;          // -
constexpr unsigned int  ZSUP        = 0x140;    // Zero suppression on send ?
constexpr unsigned int  DSEND       = 0x171;    // Decision on send  ?
constexpr unsigned int  DAC_SYNC    = 0x119;    // DAC Sync
constexpr unsigned int  RESET_DRS   = 0x111;    // DSR reset
constexpr unsigned int  RESET_INTR  = 0x110;    // FPGA internal reset
constexpr unsigned int  DATA_LAT    = 0x11A;    // ADC data latency
constexpr unsigned int  DATA_RESET  = 0x112;    // emties ADC data
constexpr unsigned int  DAISY_CHAIN = 0x153;    // Daisy chain selection, value 0 -farthest 1-otherwise
