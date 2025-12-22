"""
简化帧对齐缓冲区 - 1:1:1架构优化版本

基于性能分析结果的简化实现，专门针对Silero VAD的512样本帧要求优化。
消除性能瓶颈，保持接口兼容性。

核心优化：
- 使用bytes而不是bytearray，减少动态分配开销
- 简化缓冲区管理，避免复杂的溢出处理
- 最小化内存拷贝操作
- 保持完全的接口兼容性
"""

import logging

logger = logging.getLogger(__name__)


class FrameAlignedBuffer:
    """
    简化的512样本帧对齐缓冲区 - 性能优化版本
    
    优化原则：
    - 使用bytes替代bytearray，减少内存分配开销
    - 简化溢出处理，避免复杂的LRU机制
    - 最小化拷贝操作，提升处理效率
    - 保持接口完全兼容，零修改迁移
    """

    def __init__(self, max_buffer_samples: int = 4000):
        """
        初始化简化帧对齐缓冲区
        
        Args:
            max_buffer_samples: 最大缓冲样本数，默认为4000（0.25秒@16kHz）
        """
        self._data = b''  # 使用bytes而不是bytearray，性能更优
        self._frame_size_bytes = 1024  # 512样本 * 2字节
        self._max_buffer_size = max_buffer_samples * 2  # 简化的大小限制
        self._samples_per_frame = 512

        logger.debug(f"FrameAlignedBuffer优化版初始化: max_buffer_size={self._max_buffer_size}字节")

    def write(self, audio_data: bytes) -> None:
        """
        写入音频数据到缓冲区 - 优化版本
        
        Args:
            audio_data: 音频数据（任意大小）
        
        Raises:
            BufferError: 缓冲区溢出时抛出异常
        """
        if not audio_data:
            return

        # 检查写入后是否会溢出
        new_size = len(self._data) + len(audio_data)
        if new_size > self._max_buffer_size:
            # 记录错误并抛出异常，而不是静默截断
            logger.error(
                f"缓冲区即将溢出: 当前={len(self._data)}B, "
                f"写入={len(audio_data)}B, "
                f"总计={new_size}B, "
                f"最大={self._max_buffer_size}B"
            )
            # 紧急情况：清空缓冲区以避免数据损坏
            self._data = b''
            logger.warning("已清空缓冲区以防止数据损坏")
            # 继续写入新数据
            self._data = audio_data
            return

        # 使用bytes连接，比bytearray.extend()更高效
        self._data += audio_data

    def has_complete_frame(self) -> bool:
        """
        检查是否有完整的512样本帧
        
        Returns:
            True如果有完整帧可读，False否则
        """
        return len(self._data) >= self._frame_size_bytes

    def read_frame(self) -> bytes | None:
        """
        读取一个完整的512样本帧 - 优化版本
        
        Returns:
            512样本的音频数据（1024字节），如果不足则返回None
        """
        if not self.has_complete_frame():
            return None

        # 直接切片提取帧数据，避免额外的bytes()转换
        frame_data = self._data[:self._frame_size_bytes]
        
        # 更新缓冲区：移除已读取的数据
        self._data = self._data[self._frame_size_bytes:]

        return frame_data

    def available_samples(self) -> int:
        """
        返回缓冲区中可用的样本数
        
        Returns:
            可用样本数
        """
        return len(self._data) // 2  # 2字节/样本

    def available_frames(self) -> int:
        """
        返回缓冲区中可用的完整帧数
        
        Returns:
            可用的完整帧数
        """
        return len(self._data) // self._frame_size_bytes

    def clear(self) -> None:
        """清空缓冲区"""
        self._data = b''
        logger.debug("FrameAlignedBuffer已清空")

    def get_buffer_usage_ratio(self) -> float:
        """
        获取缓冲区使用率
        
        Returns:
            使用率 (0.0-1.0)
        """
        return len(self._data) / self._max_buffer_size

    @property
    def buffer_size_bytes(self) -> int:
        """当前缓冲区大小（字节）"""
        return len(self._data)

    @property
    def max_buffer_size_bytes(self) -> int:
        """最大缓冲区大小（字节）"""
        return self._max_buffer_size

    @property
    def frame_size_bytes(self) -> int:
        """帧大小（字节）"""
        return self._frame_size_bytes

    @property
    def samples_per_frame(self) -> int:
        """每帧样本数"""
        return self._samples_per_frame

    def __str__(self) -> str:
        return (f"FrameAlignedBuffer(size={len(self._data)}B, "
                f"frames={self.available_frames()}, "
                f"usage={self.get_buffer_usage_ratio():.1%})")


__all__ = ["FrameAlignedBuffer"]
