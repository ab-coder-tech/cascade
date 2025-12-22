"""
流式处理器核心模块 - 简化版

实现1:1:1:1架构：1个StreamProcessor = 1个独立VAD模型 + VADIterator + Buffer + 状态机
完全独立的实例设计，无锁无竞争，真正的简洁高效。
"""
import asyncio
import logging
import os
import time
from collections.abc import AsyncIterator
from enum import Enum
from typing import Optional, TYPE_CHECKING

import numpy as np
import torch

from .buffer import FrameAlignedBuffer
from .errors import (
    CascadeError,
    ErrorCode,
    ErrorSeverity,
    AudioFormatError,
)
from .types import (
    AUDIO_FRAME_SIZE,
    AudioFrame,
    CascadeResult,
    Config,
    ProcessorStats,
    SystemState,
    SpeechSegment,
)

if TYPE_CHECKING:
    pass

from .interruption import InterruptionManager

# 安全配置常量
MAX_CHUNK_SIZE = 512 * 1024  # 512KB - 防止意外的超大数据
MIN_CHUNK_SIZE = 2  # 最小2字节（1个样本）

logger = logging.getLogger(__name__)


class VADState(Enum):
    """VAD状态枚举"""
    IDLE = "idle"           # 空闲状态，等待语音开始
    COLLECTING = "collecting"  # 收集状态，正在收集语音帧


class SpeechCollector:
    """
    语音帧收集器
    
    负责收集从VAD检测到start到end之间的所有音频帧，
    并在检测到end时生成完整的语音段。
    """

    def __init__(self, segment_id: int):
        """
        初始化语音帧收集器
        
        Args:
            segment_id: 语音段ID
        """
        self.segment_id = segment_id
        self.frames: list[AudioFrame] = []
        self.start_timestamp_ms: float | None = None
        self.start_vad_result: dict | None = None
        self.is_collecting = False

        logger.debug(f"SpeechCollector {segment_id} 初始化")

    def start_collection(self, start_frame: AudioFrame) -> None:
        """
        开始收集语音帧
        
        Args:
            start_frame: 包含start VAD结果的帧
        """
        if self.is_collecting:
            logger.warning(f"SpeechCollector {self.segment_id} 已在收集中，忽略新的start")
            return

        self.frames = [start_frame]
        self.start_timestamp_ms = start_frame.timestamp_ms
        self.start_vad_result = start_frame.vad_result
        self.is_collecting = True

        logger.debug(f"SpeechCollector {self.segment_id} 开始收集，时间戳: {self.start_timestamp_ms}ms")

    def add_frame(self, frame: AudioFrame) -> None:
        """
        添加音频帧到收集器
        
        Args:
            frame: 要添加的音频帧
        """
        if not self.is_collecting:
            logger.warning(f"SpeechCollector {self.segment_id} 未在收集状态，忽略帧")
            return

        self.frames.append(frame)
        logger.debug(f"SpeechCollector {self.segment_id} 添加帧 {frame.frame_id}")

    def end_collection(self, end_frame: AudioFrame) -> SpeechSegment:
        """
        结束收集并生成语音段
        
        Args:
            end_frame: 包含end VAD结果的帧
            
        Returns:
            完整的语音段
        """
        if not self.is_collecting:
            raise ValueError(f"SpeechCollector {self.segment_id} 未在收集状态")

        if self.start_timestamp_ms is None or self.start_vad_result is None:
            raise ValueError(f"SpeechCollector {self.segment_id} 缺少开始信息")

        if end_frame.vad_result is None:
            raise ValueError("结束帧缺少VAD结果")

        # 添加结束帧
        self.frames.append(end_frame)

        # 合并所有音频数据
        audio_data = b''.join(frame.audio_data for frame in self.frames)

        # 创建语音段
        segment = SpeechSegment(
            segment_id=self.segment_id,
            audio_data=audio_data,
            start_timestamp_ms=self.start_timestamp_ms,
            end_timestamp_ms=end_frame.timestamp_ms,
            frame_count=len(self.frames),
            start_vad_result=self.start_vad_result,
            end_vad_result=end_frame.vad_result
        )

        # 重置状态
        self.is_collecting = False
        logger.info(f"SpeechCollector {self.segment_id} 完成收集: {segment}")

        return segment

    def reset(self) -> None:
        """重置收集器状态"""
        self.frames.clear()
        self.start_timestamp_ms = None
        self.start_vad_result = None
        self.is_collecting = False
        logger.debug(f"SpeechCollector {self.segment_id} 重置")

    @property
    def frame_count(self) -> int:
        """当前收集的帧数"""
        return len(self.frames)

    @property
    def duration_ms(self) -> float:
        """当前收集的时长(ms)"""
        if not self.frames or self.start_timestamp_ms is None:
            return 0.0
        return self.frames[-1].timestamp_ms - self.start_timestamp_ms

    @property
    def memory_usage_bytes(self) -> int:
        """当前内存使用量(字节)"""
        return sum(len(frame.audio_data) for frame in self.frames)

    def __str__(self) -> str:
        status = "collecting" if self.is_collecting else "idle"
        return f"SpeechCollector(id={self.segment_id}, frames={self.frame_count}, {status})"


class VADStateMachine:
    """
    VAD状态机
    
    基于silero-vad的输出管理语音检测状态转换：
    - None: 无语音活动
    - {'start': timestamp}: 语音开始
    - {'end': timestamp}: 语音结束
    """

    def __init__(self, instance_id: str, interruption_manager: 'InterruptionManager | None' = None):
        """
        初始化VAD状态机
        
        Args:
            instance_id: 实例ID
            interruption_manager: 打断管理器（可选）
        """
        self.instance_id = instance_id
        self.state = VADState.IDLE
        self.current_collector: SpeechCollector | None = None
        self.segment_counter = 0
        self.interruption_manager = interruption_manager

        logger.debug(f"VADStateMachine {instance_id} 初始化")

    def process_frame(self, frame: AudioFrame) -> CascadeResult | None:
        """
        处理音频帧并管理状态转换
        
        Args:
            frame: 输入音频帧
            
        Returns:
            处理结果，可能是单帧或语音段
        """
        start_time = time.time()

        try:
            result = self._handle_vad_result(frame)

            # 计算处理时间
            processing_time_ms = (time.time() - start_time) * 1000

            if result:
                result.processing_time_ms = processing_time_ms
                result.instance_id = self.instance_id

            return result

        except Exception as e:
            logger.error(f"VADStateMachine {self.instance_id} 处理帧失败: {e}")
            raise

    def _handle_vad_result(self, frame: AudioFrame) -> CascadeResult | None:
        """
        处理VAD结果并执行状态转换
        
        Args:
            frame: 音频帧
            
        Returns:
            处理结果
        """
        vad_result = frame.vad_result

        if vad_result is None:
            # 无语音活动
            return self._handle_no_speech(frame)

        elif 'start' in vad_result:
            # 语音开始
            return self._handle_speech_start(frame)

        elif 'end' in vad_result:
            # 语音结束
            return self._handle_speech_end(frame)

        else:
            logger.warning(f"未知VAD结果格式: {vad_result}")
            return self._handle_no_speech(frame)

    def _handle_no_speech(self, frame: AudioFrame) -> CascadeResult | None:
        """
        处理无语音状态
        
        Args:
            frame: 音频帧
            
        Returns:
            单帧结果或None
        """
        if self.state == VADState.COLLECTING and self.current_collector:
            # 在收集状态中，继续添加帧
            self.current_collector.add_frame(frame)
            logger.debug(f"VADStateMachine {self.instance_id} 收集中添加帧 {frame.frame_id}")
            return None

        # 空闲状态，返回单帧
        return CascadeResult(
            result_type="frame",
            frame=frame,
            segment=None,
            processing_time_ms=0.0,  # 将在上层设置
            instance_id=self.instance_id
        )

    def _handle_speech_start(self, frame: AudioFrame) -> CascadeResult | None:
        """
        处理语音开始
        
        Args:
            frame: 包含start VAD结果的帧
            
        Returns:
            CascadeResult（可能是打断事件）或 None
        """
        # 通知打断管理器
        interruption_event = None
        if self.interruption_manager:
            interruption_event = self.interruption_manager.on_speech_start(frame.timestamp_ms)
            
            # 状态同步卫兵（Gatekeeper）
            # 如果管理器拒绝进入收集状态（例如间隔太短或策略限制），
            # 状态机也必须忽略这次语音，防止状态分裂（Zombie State）
            if self.interruption_manager.get_state() != SystemState.COLLECTING:
                logger.debug(
                    f"VADStateMachine {self.instance_id} 忽略语音开始: "
                    f"Manager状态为 {self.interruption_manager.get_state().value}"
                )
                return None
        
        if self.state == VADState.COLLECTING:
            logger.warning(f"VADStateMachine {self.instance_id} 已在收集状态，忽略新的start")
            return None

        # 创建新的收集器并开始收集
        self.segment_counter += 1
        self.current_collector = SpeechCollector(self.segment_counter)
        self.current_collector.start_collection(frame)
        self.state = VADState.COLLECTING

        logger.info(f"VADStateMachine {self.instance_id} 开始收集语音段 {self.segment_counter}")
        
        # 如果有打断事件，返回打断事件；否则返回None
        if interruption_event:
            logger.info(f"VADStateMachine {self.instance_id} 检测到打断")
            return CascadeResult(
                result_type="interruption",
                frame=None,
                segment=None,
                interruption=interruption_event,
                processing_time_ms=0.0,
                instance_id=self.instance_id
            )
        
        return None

    def _handle_speech_end(self, frame: AudioFrame) -> CascadeResult | None:
        """
        处理语音结束
        
        Args:
            frame: 包含end VAD结果的帧
            
        Returns:
            语音段结果
        """
        # 通知打断管理器
        if self.interruption_manager:
            self.interruption_manager.on_speech_end(frame.timestamp_ms)
        
        if self.state != VADState.COLLECTING or not self.current_collector:
            logger.warning(f"VADStateMachine {self.instance_id} 未在收集状态，忽略end")
            return None

        # 结束收集并生成语音段
        segment = self.current_collector.end_collection(frame)

        # 重置状态
        self.state = VADState.IDLE
        self.current_collector = None

        logger.info(f"VADStateMachine {self.instance_id} 完成语音段 {segment.segment_id}")
        
        return CascadeResult(
            result_type="segment",
            frame=None,
            segment=segment,
            processing_time_ms=0.0,  # 将在上层设置
            instance_id=self.instance_id
        )

    def reset(self) -> None:
        """重置状态机"""
        if self.current_collector:
            self.current_collector.reset()

        self.state = VADState.IDLE
        self.current_collector = None
        self.segment_counter = 0

        logger.info(f"VADStateMachine {self.instance_id} 重置")

    @property
    def is_collecting(self) -> bool:
        """是否正在收集语音"""
        return self.state == VADState.COLLECTING

    @property
    def current_segment_id(self) -> int | None:
        """当前语音段ID"""
        if self.current_collector:
            return self.current_collector.segment_id
        return None

    @property
    def current_frame_count(self) -> int:
        """当前收集的帧数"""
        if self.current_collector:
            return self.current_collector.frame_count
        return 0

    def __str__(self) -> str:
        if self.is_collecting:
            return f"VADStateMachine({self.instance_id}, collecting segment {self.current_segment_id})"
        else:
            return f"VADStateMachine({self.instance_id}, idle)"


class StreamProcessor:
    """
    流式处理器 - 简化的1:1:1:1架构
    
    每个实例对应一个WebSocket连接，拥有完全独立的VAD模型和组件。
    无锁无竞争设计，真正的简洁高效。
    
    核心组件：
    - 独立的VAD模型实例
    - 独立的VADIterator实例
    - FrameAlignedBuffer（同步，快速）
    - VADStateMachine（同步，快速）
    
    处理流程：
    1. 音频数据写入FrameAlignedBuffer（同步）
    2. 读取完整512样本帧（同步）
    3. VAD推理（使用asyncio.to_thread在线程池中执行）
    4. 状态机处理（同步）
    5. 返回结果
    """

    def __init__(self, config: Optional[Config] = None):
        """
        初始化流式处理器（不执行实际初始化）
        
        Args:
            config: 处理器配置
        """
        if not config:
            config = Config()
        
        # 验证配置
        self._validate_config(config)
        
        self.config = config
        
        # 打断管理器（先初始化，因为state_machine需要它）
        self.interruption_manager = InterruptionManager(
            config.interruption_config
        )
        
        # 1:1:1:1绑定组件
        self.frame_buffer = FrameAlignedBuffer(max_buffer_samples=128000)
        self.state_machine = VADStateMachine("stream_processor", self.interruption_manager)
        
        # VAD组件（延迟初始化）
        self.model = None
        self.vad_iterator = None
        
        # 统计信息
        self.frame_counter = 0
        self.total_chunks_processed = 0
        self.total_processing_time_ms = 0.0
        self.speech_segments_count = 0
        self.single_frames_count = 0
        self.error_count = 0
        
        # 性能监控
        self.processing_times = []  # 最近100次处理时间
        self.max_processing_times = 100
        
        # 并发控制
        self._processing_semaphore = asyncio.Semaphore(50)  # 最多50个并发处理
        self._last_process_time = 0.0  # 上次处理时间
        self._min_process_interval = 0.0  # 不限制调用间隔
        
        self.is_initialized = False
        
        logger.info("StreamProcessor创建完成（简化版1:1:1:1架构）")
    
    def _validate_config(self, config: Config) -> None:
        """验证配置"""
        if config.vad_threshold < 0 or config.vad_threshold > 1:
            raise CascadeError(
                f"vad_threshold必须在0-1之间，当前值: {config.vad_threshold}",
                ErrorCode.INVALID_CONFIG,
                ErrorSeverity.HIGH
            )
    
    async def initialize(self) -> None:
        """
        异步初始化VAD组件
        
        关键：每个实例加载自己的独立模型，避免并发问题
        """
        if self.is_initialized:
            logger.warning("StreamProcessor已经初始化")
            return
        
        try:
            # 加载独立的VAD模型（在线程池中执行）
            from silero_vad import load_silero_vad, VADIterator
            
            logger.info("开始加载独立VAD模型...")
            self.model = await asyncio.to_thread(
                load_silero_vad,
                onnx=False  # 使用PyTorch模式
            )
            
            # 创建独立的VADIterator实例
            self.vad_iterator = VADIterator(
                self.model,  # 使用独立模型
                sampling_rate=16000,
                threshold=self.config.vad_threshold,
                min_silence_duration_ms=self.config.min_silence_duration_ms,
                speech_pad_ms=self.config.speech_pad_ms
            )
            
            self.is_initialized = True
            logger.info("StreamProcessor初始化完成（独立模型已加载）")
            
        except Exception as e:
            logger.error(f"StreamProcessor初始化失败: {e}")
            raise CascadeError(
                f"初始化失败: {e}",
                ErrorCode.INITIALIZATION_FAILED,
                ErrorSeverity.HIGH
            ) from e
    
    async def process_chunk(self, audio_data: bytes) -> list[CascadeResult]:
        """
        处理音频块
        
        带有并发控制和速率限制，防止资源耗尽。
        
        Args:
            audio_data: 音频数据（任意大小）
            
        Returns:
            处理结果列表
        """
        if self.vad_iterator is None:
            raise CascadeError(
                "StreamProcessor未初始化，请先调用initialize()",
                ErrorCode.INVALID_STATE,
                ErrorSeverity.HIGH
            )
        
        # 安全验证：输入数据大小和格式
        self._validate_audio_chunk(audio_data)
        
        # 并发控制：限制同时处理的数量（防止资源耗尽）
        async with self._processing_semaphore:
            results = await self._process_chunk_internal(audio_data)
        
        self._last_process_time = time.time()
        return results
    
    async def _process_chunk_internal(self, audio_data: bytes) -> list[CascadeResult]:
        """
        内部处理逻辑（在并发控制下执行）
        
        Args:
            audio_data: 音频数据
            
        Returns:
            处理结果列表
        """
        results = []
        start_time = time.time()
        
        try:
            # 1. 写入缓冲区（同步，快速）
            self.frame_buffer.write(audio_data)
            
            # 2. 处理所有完整帧
            while self.frame_buffer.has_complete_frame():
                # 读取帧（同步，快速）
                frame_data = self.frame_buffer.read_frame()
                
                if not frame_data:
                    break
                
                # 准备数据（同步，快速）
                audio_array = np.frombuffer(
                    frame_data,
                    dtype=np.int16
                ).astype(np.float32) / 32768.0
                
                audio_tensor = torch.from_numpy(audio_array)
                
                # VAD推理（异步，CPU密集型，在线程池中执行）
                # 由于每个StreamProcessor有独立的model和vad_iterator，
                # 多个StreamProcessor可以并发调用，互不干扰
                vad_result = await asyncio.to_thread(
                    self.vad_iterator,
                    audio_tensor
                )
                
                # 状态机处理（同步，快速逻辑）
                self.frame_counter += 1
                timestamp_ms = self.frame_counter * 32.0  # 32ms per frame
                
                frame = AudioFrame(
                    frame_id=self.frame_counter,
                    audio_data=frame_data,
                    timestamp_ms=timestamp_ms,
                    vad_result=vad_result
                )
                
                result = self.state_machine.process_frame(frame)
                
                if result:
                    # 更新统计
                    if result.is_speech_segment:
                        self.speech_segments_count += 1
                    else:
                        self.single_frames_count += 1
                    
                    results.append(result)
            
            # 记录处理时间
            processing_time = (time.time() - start_time) * 1000
            self.total_chunks_processed += 1
            self.total_processing_time_ms += processing_time
            self._record_processing_time(processing_time)
            
            return results
            
        except Exception as e:
            self.error_count += 1
            logger.error(f"处理音频块失败: {e}")
            raise CascadeError(
                f"处理音频块失败: {e}",
                ErrorCode.PROCESSING_FAILED,
                ErrorSeverity.HIGH
            ) from e
    
    async def process_file(self, file_path: str) -> AsyncIterator[CascadeResult]:
        """
        处理音频文件
        
        Args:
            file_path: 音频文件路径
            
        Yields:
            CascadeResult: 处理结果
        """
        # 确保已初始化
        if not self.is_initialized:
            await self.initialize()
        
        try:
            # 验证文件存在
            if not os.path.exists(file_path):
                raise CascadeError(
                    f"音频文件不存在: {file_path}",
                    ErrorCode.INVALID_INPUT,
                    ErrorSeverity.HIGH,
                    {"file_path": file_path}
                )
            
            # 读取音频文件
            audio_data = self._read_audio_file(file_path, target_sample_rate=16000)
            audio_frames = self._generate_audio_frames(audio_data)
            
            # 逐帧处理
            for frame_data in audio_frames:
                results = await self.process_chunk(frame_data)
                for result in results:
                    yield result
                    
        except Exception as e:
            raise CascadeError(
                f"处理音频文件失败: {e}",
                ErrorCode.PROCESSING_FAILED,
                ErrorSeverity.HIGH,
                {"file_path": file_path}
            ) from e
    
    async def process_stream(
        self,
        audio_stream: AsyncIterator[bytes]
    ) -> AsyncIterator[CascadeResult]:
        """
        处理音频流
        
        Args:
            audio_stream: 音频数据流
            
        Yields:
            处理结果
        """
        # 确保已初始化
        if not self.is_initialized:
            await self.initialize()
        
        try:
            async for audio_chunk in audio_stream:
                results = await self.process_chunk(audio_chunk)
                for result in results:
                    yield result
                    
        except Exception as e:
            raise CascadeError(
                f"处理音频流失败: {e}",
                ErrorCode.PROCESSING_FAILED,
                ErrorSeverity.HIGH
            ) from e
    
    async def close(self) -> None:
        """
        清理资源并释放内存
        
        显式清理所有资源，包括PyTorch模型、缓冲区和状态机。
        使用超时保护防止清理过程阻塞。
        """
        try:
            # 1. 清理VAD迭代器状态
            if self.vad_iterator:
                try:
                    self.vad_iterator.reset_states()
                except Exception as e:
                    logger.warning(f"重置VAD迭代器状态失败: {e}")
            
            # 2. 显式清理PyTorch模型
            if self.model is not None:
                try:
                    # 如果使用GPU，清理CUDA缓存
                    if torch.cuda.is_available() and next(self.model.parameters(), None) is not None:
                        if next(self.model.parameters()).is_cuda:
                            torch.cuda.empty_cache()
                            logger.debug("已清理CUDA缓存")
                except Exception as e:
                    logger.warning(f"清理CUDA缓存失败: {e}")
                
                # 删除模型引用
                del self.model
                self.model = None
            
            # 3. 清理VAD迭代器
            if self.vad_iterator is not None:
                del self.vad_iterator
                self.vad_iterator = None
            
            # 4. 清理其他组件
            if self.frame_buffer:
                self.frame_buffer.clear()
            
            if self.state_machine:
                self.state_machine.reset()
            
            # 5. 清理统计数据
            self.processing_times.clear()
            
            # 6. 强制垃圾回收（在资源密集型应用中有助于及时释放内存）
            import gc
            gc.collect()
            
            self.is_initialized = False
            logger.info("StreamProcessor已清理，所有资源已释放")
            
        except Exception as e:
            logger.error(f"清理资源时发生错误: {e}")
            # 即使清理失败，也要标记为未初始化
            self.is_initialized = False
            raise CascadeError(
                f"资源清理失败: {e}",
                ErrorCode.CLEANUP_FAILED,
                ErrorSeverity.HIGH
            ) from e
    
    def _read_audio_file(self, file_path: str, target_sample_rate: int = 16000):
        """
        读取音频文件
        
        Args:
            file_path: 音频文件路径
            target_sample_rate: 目标采样率
            
        Returns:
            音频数据数组 (torch.Tensor)
        """
        try:
            import soundfile as sf
            from scipy import signal
            
            # 使用 soundfile 读取音频（兼容新版 torchaudio）
            audio_data, sample_rate = sf.read(file_path)
            
            # 如果是多声道，转换为单声道
            if len(audio_data.shape) > 1:
                audio_data = audio_data.mean(axis=1)
            
            # 如果采样率不匹配，进行重采样
            if sample_rate != target_sample_rate:
                num_samples = int(len(audio_data) * target_sample_rate / sample_rate)
                audio_data = signal.resample(audio_data, num_samples)
            
            # 转换为 float32 并归一化到 [-1, 1]
            audio_data = audio_data.astype(np.float32)
            if audio_data.max() > 1.0 or audio_data.min() < -1.0:
                audio_data = audio_data / max(abs(audio_data.max()), abs(audio_data.min()))
            
            # 转换为 torch.Tensor
            return torch.from_numpy(audio_data)
            
        except ImportError as err:
            raise ImportError("soundfile 或 scipy 未安装，无法读取音频文件") from err
    
    def _generate_audio_frames(self, audio_data, frame_size: int = 512) -> list:
        """
        生成512样本的音频帧
        
        Args:
            audio_data: 音频数据
            frame_size: 帧大小（样本数）
            
        Returns:
            音频帧列表（bytes格式）
        """
        frames = []
        
        # 按512样本分块
        for i in range(0, len(audio_data), frame_size):
            frame = audio_data[i:i + frame_size]
            
            # 如果最后一帧不足512样本，跳过
            if len(frame) < frame_size:
                break
            
            # 如果是PyTorch Tensor，先转换为numpy数组
            if hasattr(frame, 'numpy') and callable(getattr(frame, 'numpy', None)):
                frame = frame.numpy()
            elif hasattr(frame, 'detach') and callable(getattr(frame, 'detach', None)):
                frame = frame.detach().numpy()
            
            frame_int16 = (frame * 32767).astype(np.int16)
            frame_bytes = frame_int16.tobytes()
            frames.append(frame_bytes)
        
        return frames
    
    def _validate_audio_chunk(self, audio_data: bytes) -> None:
        """
        验证音频块的基本完整性
        
        Args:
            audio_data: 待验证的音频数据
            
        Raises:
            CascadeError: 数据验证失败
        """
        # 检查数据大小
        data_size = len(audio_data)
        
        if data_size == 0:
            # 空数据直接返回，不抛异常
            return
        
        # 只检查极端情况（超过512KB）
        if data_size > MAX_CHUNK_SIZE:
            logger.error(
                f"音频块异常大: {data_size} bytes (最大建议: {MAX_CHUNK_SIZE} bytes)"
            )
            raise CascadeError(
                f"音频块过大: {data_size} bytes",
                ErrorCode.INVALID_INPUT,
                ErrorSeverity.HIGH,
                {"chunk_size": data_size, "max_size": MAX_CHUNK_SIZE}
            )
        
        # 验证16-bit对齐（每个样本2字节）- 这是必须的
        if data_size % 2 != 0:
            raise CascadeError(
                f"音频数据格式错误: 大小必须是偶数字节（16-bit采样），当前: {data_size} bytes",
                ErrorCode.AUDIO_CORRUPTION,
                ErrorSeverity.HIGH,
                {"chunk_size": data_size}
            )
        
        # 调试信息：全静音数据
        if data_size >= 1024 and audio_data == b'\x00' * data_size:
            logger.debug(f"检测到全静音音频数据: {data_size} bytes")
    
    def _record_processing_time(self, processing_time_ms: float) -> None:
        """记录处理时间"""
        self.processing_times.append(processing_time_ms)
        
        # 保持最近的处理时间记录
        if len(self.processing_times) > self.max_processing_times:
            self.processing_times.pop(0)
    
    def get_stats(self) -> ProcessorStats:
        """
        获取处理器统计信息
        
        包含性能告警检测，当指标异常时记录警告日志。
        """
        # 计算平均处理时间
        avg_processing_time = 0.0
        if self.total_chunks_processed > 0:
            avg_processing_time = self.total_processing_time_ms / self.total_chunks_processed
        
        # 计算语音比例
        total_results = self.speech_segments_count + self.single_frames_count
        speech_ratio = 0.0
        if total_results > 0:
            speech_ratio = self.speech_segments_count / total_results
        
        # 计算吞吐量
        throughput = 0.0
        if self.total_processing_time_ms > 0:
            throughput = self.total_chunks_processed / (self.total_processing_time_ms / 1000.0)
        
        # 计算错误率
        error_rate = 0.0
        if self.total_chunks_processed > 0:
            error_rate = self.error_count / self.total_chunks_processed
        
        # 估算内存使用（单实例约80MB）
        memory_usage_mb = 80.0 if self.is_initialized else 0.0
        
        # 性能告警检测
        if self.total_chunks_processed > 10:  # 至少处理10个块后才告警
            if avg_processing_time > 100:
                logger.warning(
                    f"⚠️ 处理时间过长: {avg_processing_time:.2f}ms "
                    f"(建议<100ms)"
                )
            
            if error_rate > 0.05:
                logger.error(
                    f"❌ 错误率过高: {error_rate:.2%} "
                    f"({self.error_count}/{self.total_chunks_processed})"
                )
            
            if throughput < 10 and self.total_chunks_processed > 100:
                logger.warning(
                    f"⚠️ 吞吐量过低: {throughput:.1f} 块/秒 "
                    f"(建议>10块/秒)"
                )
        
        return ProcessorStats(
            total_chunks_processed=self.total_chunks_processed,
            total_processing_time_ms=self.total_processing_time_ms,
            average_processing_time_ms=avg_processing_time,
            speech_segments=self.speech_segments_count,
            single_frames=self.single_frames_count,
            speech_ratio=speech_ratio,
            throughput_chunks_per_second=throughput,
            memory_usage_mb=memory_usage_mb,
            error_count=self.error_count,
            error_rate=error_rate
        )
    
    def reset_stats(self) -> None:
        """重置统计信息"""
        self.total_chunks_processed = 0
        self.total_processing_time_ms = 0.0
        self.speech_segments_count = 0
        self.single_frames_count = 0
        self.error_count = 0
        self.processing_times.clear()
        logger.info("统计信息已重置")
    
    def set_system_state(self, state: SystemState) -> None:
        """
        外部设置系统状态
        
        Args:
            state: 要设置的系统状态
        """
        self.interruption_manager.set_state(state)
    
    def get_system_state(self) -> SystemState:
        """
        获取当前系统状态
        
        Returns:
            当前系统状态
        """
        return self.interruption_manager.get_state()
    
    def get_interruption_stats(self) -> dict:
        """
        获取打断统计信息
        
        Returns:
            打断统计信息字典
        """
        return self.interruption_manager.get_stats()
    
    def __str__(self) -> str:
        status = "initialized" if self.is_initialized else "not initialized"
        return f"StreamProcessor({status}, frames={self.frame_counter})"
    
    async def __aenter__(self):
        """异步上下文管理器入口"""
        await self.initialize()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
        异步上下文管理器退出
        
        添加超时保护，防止清理过程阻塞。
        """
        try:
            # 使用超时保护，防止清理过程无限阻塞
            await asyncio.wait_for(self.close(), timeout=10.0)
        except asyncio.TimeoutError:
            logger.error("关闭StreamProcessor超时（10秒），强制退出")
            self.is_initialized = False
        except Exception as e:
            logger.error(f"退出上下文管理器时发生错误: {e}")
            # 确保资源被标记为已清理
            self.is_initialized = False
