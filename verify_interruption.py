"""
打断功能验证脚本 - 使用真实音频文件

测试逻辑：
1. 读取包含两段语音的真实音频文件。
2. 第一段语音正常处理 (IDLE -> COLLECTING -> IDLE)。
3. 在第一段语音结束后，模拟系统进入 PROCESSING -> RESPONDING 状态。
4. 当第二段语音开始时，预期系统处于 RESPONDING 状态。
5. 验证是否成功触发了 INTERRUPT 事件，而不是普通的 Speech Segment。
"""

import asyncio
import logging
import sys
import numpy as np
import soundfile as sf

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)

from cascade.stream import (
    Config, InterruptionConfig, StreamProcessor, SystemState
)

AUDIO_FILE = "/home/justin/workspace/cascade/我现在开始录音，理论上会有两个文件.wav"

async def main():
    print("\n=== 开始打断功能验证 ===")
    print(f"音频文件: {AUDIO_FILE}")
    
    # 1. 准备音频数据
    try:
        audio_data, sample_rate = sf.read(AUDIO_FILE, dtype='float32')
        print(f"音频加载成功: {len(audio_data)} 样本, {sample_rate} Hz, {len(audio_data)/sample_rate:.2f} 秒")
        
        if sample_rate != 16000:
            print("需要重采样到 16000 Hz...")
            from scipy import signal
            audio_data = signal.resample(audio_data, int(len(audio_data) * 16000 / sample_rate))
            
        # 转为 int16 bytes
        audio_int16 = (audio_data * 32767).astype(np.int16)
        audio_bytes = audio_int16.tobytes()
    except Exception as e:
        print(f"❌ 音频读取失败: {e}")
        return

    # 2. 配置处理器
    config = Config(
        vad_threshold=0.5,
        interruption_config=InterruptionConfig(
            enable_interruption=True,
            min_interval_ms=500
        )
    )

    # 3. 运行模拟流程
    async with StreamProcessor(config) as processor:
        print("Processor 初始化完成\n")
        
        chunk_size = 1024 # 每次发送 1024 字节
        total_bytes = len(audio_bytes)
        segment_count = 0
        interruption_triggered = False
        
        # 模拟状态变量
        simulated_responding = False

        for i in range(0, total_bytes, chunk_size):
            chunk = audio_bytes[i:i+chunk_size]
            results = await processor.process_chunk(chunk)
            
            # --- 状态模拟逻辑 ---
            # 如果已经处理完第一个语音段(segment_count >= 1)，并且还没进入过模拟状态
            # 我们强制把系统状态设为 RESPONDING，假装系统正在说话
            if segment_count == 1 and not simulated_responding:
                # 检查当前是否空闲，如果是空闲，我们模拟系统开始回复
                if processor.get_system_state() == SystemState.IDLE:
                    print("\n>>> [模拟] 第一段语音结束，系统开始思考并回复...")
                    processor.set_system_state(SystemState.PROCESSING)
                    processor.set_system_state(SystemState.RESPONDING)
                    print(f">>> [状态] 当前系统状态已切换为: {processor.get_system_state().value} (等待打断)\n")
                    simulated_responding = True

            # --- 结果处理 ---
            for result in results:
                if result.is_interruption:
                    print(f"\n🛑 [成功] 检测到打断事件!")
                    print(f"   时间戳: {result.interruption.timestamp_ms:.0f}ms")
                    print(f"   被打断的状态: {result.interruption.system_state.value}")
                    interruption_triggered = True
                    
                    # 模拟：被打断后，业务逻辑应该停止回复，并准备聆听
                    # 注意：Manager会自动切换到 COLLECTING，不需要我们手动切
                    # 但业务层应该知道自己被打断了
                    
                elif result.is_speech_segment:
                    segment_count += 1
                    seg = result.segment
                    print(f"🎤 [语音段 #{segment_count}] {seg.start_timestamp_ms:.0f}ms -> {seg.end_timestamp_ms:.0f}ms (时长: {seg.duration_ms:.0f}ms)")
                    
                    # 如果这是打断后的语音段，说明打断流程完整走通了
                    if interruption_triggered and segment_count == 2:
                        print("   (这是打断系统回复后录制的语音)")
                        # 语音结束，业务处理完成，回到IDLE
                        processor.set_system_state(SystemState.IDLE)

    print("\n=== 验证结果总结 ===")
    if interruption_triggered:
        print("✅ 验证通过: 成功触发了打断事件。")
    else:
        print("❌ 验证失败: 未触发打断事件 (可能是时间配合问题或逻辑问题)。")

if __name__ == "__main__":
    asyncio.run(main())
