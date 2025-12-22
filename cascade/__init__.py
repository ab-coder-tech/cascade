"""
Cascade: 高性能异步流式VAD处理库

Cascade是一个专为语音活动检测(VAD)设计的高性能、低延迟音频流处理库。
基于StreamProcessor核心架构，提供简洁的异步流式处理能力。

核心特性:
- 流式处理: 基于VAD状态机的异步流式音频处理
- 语音段检测: 自动检测和收集完整语音段
- 异步设计: 基于asyncio的高并发处理能力
- 低延迟: 优化的缓冲区和处理流程
- 打断检测: 支持用户语音打断检测

快速开始:
    >>> import cascade
    >>> # 零配置使用
    >>> results = await cascade.process_audio_file("audio.wav")
    >>> print(f"检测到 {len(results)} 个结果")
    
    >>> # 流式处理
    >>> async with cascade.StreamProcessor() as processor:
    ...     async for result in processor.process_stream(audio_stream):
    ...         if result.is_speech_segment:
    ...             print(f"语音段: {result.segment.duration_ms:.0f}ms")
    ...         else:
    ...             print(f"单帧: {result.frame.timestamp_ms:.0f}ms")
"""

# 版本信息
__version__ = "2.0.0"
__author__ = "Xucailiang"
__license__ = "MIT"
__email__ = "xucailiang.ai@gmail.com"

import logging
import os
import platform
import sys

logger = logging.getLogger(__name__)

# 从新的扁平化模块导入核心类型
from .types import (
    # 常量
    AUDIO_SAMPLE_RATE,
    AUDIO_FRAME_SIZE,
    AUDIO_FRAME_DURATION_MS,
    AUDIO_CHANNELS,
    AUDIO_SAMPLE_WIDTH,
    # 枚举
    SystemState,
    # 配置
    Config,
    InterruptionConfig,
    # 数据模型
    AudioFrame,
    SpeechSegment,
    CascadeResult,
    InterruptionEvent,
    ProcessorStats,
)

# 从新模块导入组件
from .buffer import FrameAlignedBuffer
from .interruption import InterruptionManager

# 从新的 errors 模块导入异常类
from .errors import (
    ErrorCode,
    ErrorSeverity,
    CascadeError,
    AudioFormatError,
    BufferError,
)

# 从新的 processor 模块导入核心处理器
from .processor import StreamProcessor, VADState, VADStateMachine, SpeechCollector


# 公开API
__all__ = [
    # 版本信息
    "__version__",

    # 核心处理器
    "StreamProcessor",

    # 配置
    "Config",
    "InterruptionConfig",

    # 数据类型
    "AudioFrame",
    "SpeechSegment",
    "CascadeResult",
    "ProcessorStats",
    "SystemState",
    "InterruptionEvent",

    # 组件
    "FrameAlignedBuffer",
    "InterruptionManager",
    "VADStateMachine",
    "VADState",
    "SpeechCollector",

    # 异常
    "CascadeError",
    "ErrorCode",
    "ErrorSeverity",
    "AudioFormatError",
    "BufferError",

    # 便捷函数
    "create_processor",
    "create_default_config",
    "process_audio_file",

    # 常量
    "AUDIO_SAMPLE_RATE",
    "AUDIO_FRAME_SIZE",
    "AUDIO_FRAME_DURATION_MS",
    "AUDIO_CHANNELS",
    "AUDIO_SAMPLE_WIDTH",
]


# 便捷函数
def create_default_config(**kwargs) -> Config:
    """
    创建默认配置
    
    Args:
        **kwargs: 配置参数覆盖
        
    Returns:
        配置对象
        
    Example:
        config = cascade.create_default_config(vad_threshold=0.7)
    """
    return Config(**kwargs)


def create_processor(**kwargs) -> StreamProcessor:
    """
    创建流式处理器的工厂函数
    
    Args:
        **kwargs: 配置参数，覆盖默认值
            - vad_threshold: float = 0.5 (VAD检测阈值)
            - max_instances: int = 5 (最大并发实例数)
            - sample_rate: int = 16000 (采样率)
            
    Returns:
        StreamProcessor: 配置好的处理器实例
        
    Example:
        # 默认配置
        processor = cascade.create_processor()
        
        # 自定义配置
        processor = cascade.create_processor(
            vad_threshold=0.7,
            max_instances=3
        )
    """
    config = create_default_config(**kwargs)
    return StreamProcessor(config)


async def process_audio_file(file_path_or_data, **kwargs):
    """
    处理音频文件的便捷函数（异步迭代器）

    Args:
        file_path_or_data: 音频文件路径或音频数据（bytes）
        **kwargs: 配置参数

    Yields:
        CascadeResult: 处理结果

    Example:
        >>> async for result in cascade.process_audio_file("audio.wav"):
        ...     if result.is_speech_segment:
        ...         print(f"语音段: {result.segment.duration_ms:.0f}ms")
        ...     else:
        ...         print(f"单帧: {result.frame.timestamp_ms:.0f}ms")
    """
    try:
        processor = create_processor(**kwargs)
        async for result in processor.process_file(str(file_path_or_data)):
            yield result
    except Exception as e:
        raise AudioFormatError(f"音频处理失败: {e}") from e


# 兼容性检查
def check_compatibility() -> dict:
    """检查系统兼容性"""
    compatibility_info = {
        "python_version": sys.version,
        "platform": platform.platform(),
        "architecture": platform.architecture(),
        "compatible": True,
        "warnings": [],
        "errors": []
    }

    # 平台检查
    supported_platforms = ["linux", "darwin", "win32"]
    if sys.platform not in supported_platforms:
        compatibility_info["warnings"].append(
            f"平台 {sys.platform} 可能不被完全支持"
        )

    return compatibility_info


# 调试信息
def get_debug_info() -> dict:
    """获取调试信息"""
    debug_info = {
        "version": __version__,
        "python_version": sys.version,
        "install_path": os.path.dirname(__file__),
        "available_backends": [],
        "dependencies": {}
    }

    # 检查可用后端
    try:
        import torch
        debug_info["available_backends"].append("silero")
        debug_info["dependencies"]["torch"] = torch.__version__
    except ImportError:
        pass

    # 检查核心依赖
    try:
        import numpy
        debug_info["dependencies"]["numpy"] = numpy.__version__
    except ImportError:
        debug_info["dependencies"]["numpy"] = "未安装"

    try:
        import pydantic
        debug_info["dependencies"]["pydantic"] = pydantic.__version__
    except ImportError:
        debug_info["dependencies"]["pydantic"] = "未安装"

    return debug_info
