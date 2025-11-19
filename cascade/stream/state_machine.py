"""
VAD状态机

基于silero-vad输出的语音活动检测状态管理。
"""

import logging
import time
from enum import Enum
from typing import TYPE_CHECKING

from .collector import SpeechCollector
from .types import AudioFrame, CascadeResult, SystemState

if TYPE_CHECKING:
    from .interruption_manager import InterruptionManager

logger = logging.getLogger(__name__)


class VADState(Enum):
    """VAD状态枚举"""
    IDLE = "idle"           # 空闲状态，等待语音开始
    COLLECTING = "collecting"  # 收集状态，正在收集语音帧


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
