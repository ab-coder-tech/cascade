"""
Cascade 流式处理器数据类型

基于VAD状态机设计的数据类型定义。
"""

from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, Field

# 音频格式常量 - 基于silero-vad要求
AUDIO_SAMPLE_RATE = 16000  # 固定16kHz
AUDIO_FRAME_SIZE = 512     # 固定512样本/帧
AUDIO_FRAME_DURATION_MS = 32.0  # 32ms/帧
AUDIO_CHANNELS = 1         # 单声道
AUDIO_SAMPLE_WIDTH = 2     # 16-bit


class SystemState(Enum):
    """系统状态枚举"""
    IDLE = "idle"                    # 空闲：等待用户输入
    COLLECTING = "collecting"        # 收集中：用户正在说话（VAD检测到语音）
    PROCESSING = "processing"        # 处理中：后端正在处理（外部服务设置）
    RESPONDING = "responding"        # 回复中：正在输出回复（外部服务设置）


class InterruptionConfig(BaseModel):
    """打断检测配置"""
    
    enable_interruption: bool = Field(
        default=True,
        description="是否启用打断检测"
    )
    
    min_interval_ms: int = Field(
        default=500,
        ge=0,
        le=5000,
        description="最小打断间隔（防止连续误判）"
    )


class InterruptionEvent(BaseModel):
    """打断事件"""
    
    event_type: Literal["start_interrupt"] = Field(
        description="事件类型：基于start的打断"
    )
    
    timestamp_ms: float = Field(
        description="打断发生的时间戳"
    )
    
    system_state: SystemState = Field(
        description="被打断时的系统状态"
    )
    
    confidence: float = Field(
        default=1.0,
        description="打断置信度（start事件固定为1.0）"
    )
    
    state_duration_ms: float = Field(
        description="当前状态持续时长"
    )


class AudioFrame(BaseModel):
    """
    单个音频帧
    
    表示512样本的音频帧和相关元数据。
    """

    # 基础信息
    frame_id: int = Field(description="帧ID")
    audio_data: bytes = Field(description="512样本音频数据")
    timestamp_ms: float = Field(description="时间戳(ms)")

    # VAD信息
    vad_result: dict[str, Any] | None = Field(default=None, description="原始VAD结果")

    # 元数据
    sample_rate: int = Field(default=AUDIO_SAMPLE_RATE, description="采样率")
    frame_size: int = Field(default=AUDIO_FRAME_SIZE, description="帧大小(样本)")

    def __str__(self) -> str:
        vad_str = str(self.vad_result) if self.vad_result else "None"
        return f"AudioFrame(id={self.frame_id}, vad={vad_str}, {self.timestamp_ms:.0f}ms)"


class SpeechSegment(BaseModel):
    """
    语音段
    
    表示从VAD检测到start到end之间的完整语音片段。
    """

    # 基础信息
    segment_id: int = Field(description="语音段ID")
    audio_data: bytes = Field(description="合并的音频数据")

    # 时间信息
    start_timestamp_ms: float = Field(description="开始时间戳(ms)")
    end_timestamp_ms: float = Field(description="结束时间戳(ms)")

    # 统计信息
    frame_count: int = Field(description="包含的帧数")

    # VAD信息
    start_vad_result: dict[str, Any] = Field(description="开始VAD结果")
    end_vad_result: dict[str, Any] = Field(description="结束VAD结果")

    # 元数据
    sample_rate: int = Field(default=AUDIO_SAMPLE_RATE, description="采样率")

    @property
    def duration_ms(self) -> float:
        """语音段时长(ms)"""
        return self.end_timestamp_ms - self.start_timestamp_ms

    def __str__(self) -> str:
        return f"SpeechSegment(id={self.segment_id}, frames={self.frame_count}, {self.duration_ms:.0f}ms)"


class CascadeResult(BaseModel):
    """
    Cascade输出结果
    
    统一的输出接口，可以是单帧或语音段。
    """

    # 结果类型
    result_type: Literal["frame", "segment", "interruption"] = Field(description="结果类型")

    # 结果数据
    frame: AudioFrame | None = Field(default=None, description="单帧结果")
    segment: SpeechSegment | None = Field(default=None, description="语音段结果")
    interruption: InterruptionEvent | None = Field(default=None, description="打断事件信息")

    # 处理信息
    processing_time_ms: float = Field(description="处理时间(ms)")
    instance_id: str = Field(description="处理实例ID")

    def __str__(self) -> str:
        if self.result_type == "frame":
            return f"CascadeResult(frame: {self.frame})"
        else:
            return f"CascadeResult(segment: {self.segment})"

    @property
    def is_speech_segment(self) -> bool:
        """是否为语音段"""
        return self.result_type == "segment"

    @property
    def is_single_frame(self) -> bool:
        """是否为单帧"""
        return self.result_type == "frame"
    
    @property
    def is_interruption(self) -> bool:
        """是否为打断事件"""
        return self.result_type == "interruption"


class Config(BaseModel):
    """
    Cascade配置类
    
    基于silero-vad优化，固定关键音频参数，简化配置。
    """

    # 音频配置 - 基于silero-vad优化（固定值）
    sample_rate: int = Field(default=AUDIO_SAMPLE_RATE, frozen=True, description="采样率(Hz)")
    frame_size: int = Field(default=AUDIO_FRAME_SIZE, frozen=True, description="VAD帧大小(样本)")
    frame_duration_ms: float = Field(default=AUDIO_FRAME_DURATION_MS, frozen=True, description="帧时长(ms)")
    channels: int = Field(default=AUDIO_CHANNELS, frozen=True, description="声道数")
    sample_width: int = Field(default=AUDIO_SAMPLE_WIDTH, frozen=True, description="采样位宽")
    supported_formats: list[str] = Field(default=["wav", "mp3"], frozen=True, description="支持的音频格式")

    # VAD配置
    vad_threshold: float = Field(default=0.5, description="VAD检测阈值", ge=0.0, le=1.0)
    speech_pad_ms: int = Field(default=100, description="语音段填充时长(ms)", ge=0, le=5000)
    min_silence_duration_ms: int = Field(default=100, description="最小静音时长(ms)", ge=0, le=10000)

    # 性能配置
    max_instances: int = Field(default=5, description="最大并发实例数", ge=1, le=20)
    buffer_size_frames: int = Field(default=64, description="缓冲区大小(帧数)", ge=8, le=256)

    # 高级配置
    enable_logging: bool = Field(default=True, description="是否启用日志")
    log_level: str = Field(default="INFO", description="日志级别")
    enable_profiling: bool = Field(default=False, description="是否启用性能分析")
    
    # 打断配置
    interruption_config: InterruptionConfig = Field(
        default_factory=InterruptionConfig,
        description="打断检测配置"
    )

    class Config:
        extra = "forbid"
        frozen = True  # 配置不可变

    @classmethod
    def create_with_overrides(cls, **kwargs) -> 'Config':
        """
        创建配置并覆盖指定参数
        
        Args:
            **kwargs: 要覆盖的配置参数
            
        Returns:
            Config: 新的配置实例
            
        Example:
            config = Config.create_with_overrides(
                vad_threshold=0.7,
                max_instances=3
            )
        """
        return cls(**kwargs)

    @property
    def buffer_size_seconds(self) -> float:
        """缓冲区大小(秒)"""
        return (self.buffer_size_frames * self.frame_duration_ms) / 1000.0


class ProcessorStats(BaseModel):
    """
    处理器统计信息
    """

    # 处理统计
    total_chunks_processed: int = Field(description="总处理块数")
    total_processing_time_ms: float = Field(description="总处理时间(ms)")
    average_processing_time_ms: float = Field(description="平均处理时间(ms)")

    # 检测统计
    speech_segments: int = Field(description="语音段数")
    single_frames: int = Field(description="单帧数")
    speech_ratio: float = Field(description="语音比例")

    # 性能统计
    throughput_chunks_per_second: float = Field(description="吞吐量(块/秒)")
    memory_usage_mb: float = Field(description="内存使用(MB)")

    # 错误统计
    error_count: int = Field(description="错误次数")
    error_rate: float = Field(description="错误率")

    def summary(self) -> str:
        """返回统计摘要"""
        return (f"处理了{self.total_chunks_processed}个块, "
                f"语音段{self.speech_segments}个, "
                f"平均处理时间{self.average_processing_time_ms:.1f}ms")
