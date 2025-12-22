/**
 * Cascade Web Demo 配置文件
 */

// 后端服务器配置
export const SERVER_CONFIG = {
  // WebSocket 服务器地址
  WS_HOST: 'localhost',
  WS_PORT: 9001,
  WS_PATH: '/ws/new',
  
  // 获取完整的 WebSocket URL
  get WS_URL() {
    return `ws://${this.WS_HOST}:${this.WS_PORT}${this.WS_PATH}`;
  }
};

// VAD 默认配置
export const DEFAULT_VAD_CONFIG = {
  vad_threshold: 0.5,
  speech_pad_ms: 100,
  min_silence_duration_ms: 100,
  sample_rate: 16000
};

export default {
  SERVER_CONFIG,
  DEFAULT_VAD_CONFIG
};
