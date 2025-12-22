"""
打断管理器

职责：
1. 维护系统当前状态
2. 自动管理状态转换（VAD start/end）
3. 检测打断条件
4. 生成打断事件
"""

import logging
import time

from .types import InterruptionConfig, InterruptionEvent, SystemState

logger = logging.getLogger(__name__)


class InterruptionManager:
    """
    打断管理器
    
    基于VAD start事件的打断检测，自动管理系统状态转换。
    """
    
    def __init__(self, config: InterruptionConfig):
        """
        初始化打断管理器
        
        Args:
            config: 打断检测配置
        """
        self.config = config
        self.current_state = SystemState.IDLE
        
        # 统一使用毫秒时间戳
        self.state_start_time = time.time() * 1000  # ms
        self.last_speech_end_time = 0.0  # ms
        
        self.interruption_count = 0
        
        logger.debug("InterruptionManager初始化完成")
    
    def on_speech_start(self, vad_timestamp_ms: float) -> InterruptionEvent | None:
        """
        处理语音开始事件（VAD start）
        
        Args:
            vad_timestamp_ms: VAD事件的时间戳（毫秒）
            
        Returns:
            InterruptionEvent 或 None
        """
        # 1. 如果在IDLE，自动切换到COLLECTING
        if self.current_state == SystemState.IDLE:
            self._transition_to(SystemState.COLLECTING)
            return None
        
        # 2. 检查间隔（使用VAD时间戳）
        interval = vad_timestamp_ms - self.last_speech_end_time
        if interval < self.config.min_interval_ms:
            return None  # 间隔太短，不触发
        
        # 3. 检查是否应该打断
        if not self.should_interrupt():
            return None
        
        # 4. 触发打断，切换到COLLECTING
        self.interruption_count += 1
        
        interruption = InterruptionEvent(
            event_type="start_interrupt",
            timestamp_ms=vad_timestamp_ms,
            system_state=self.current_state,
            confidence=1.0,
            state_duration_ms=vad_timestamp_ms - self.state_start_time
        )
        
        logger.info(
            f"🛑 触发打断: 状态={self.current_state.value}, "
            f"时间戳={vad_timestamp_ms:.0f}ms, "
            f"间隔={interval:.0f}ms"
        )
        
        self._transition_to(SystemState.COLLECTING)
        return interruption
    
    def on_speech_end(self, vad_timestamp_ms: float) -> None:
        """
        处理语音结束事件（VAD end）
        
        Args:
            vad_timestamp_ms: VAD事件的时间戳（毫秒）
        """
        self.last_speech_end_time = vad_timestamp_ms
        
        # 如果在COLLECTING，自动切换回IDLE
        if self.current_state == SystemState.COLLECTING:
            self._transition_to(SystemState.IDLE)
    
    def should_interrupt(self) -> bool:
        """判断是否应该触发打断"""
        # 只在PROCESSING/RESPONDING状态下可以打断
        if self.current_state not in [SystemState.PROCESSING, SystemState.RESPONDING]:
            return False
        
        if not self.config.enable_interruption:
            return False
        
        return True
    
    def set_state(self, state: SystemState) -> None:
        """
        外部显式设置状态（由外部服务调用）
        
        使用场景：
        - ASR识别完成后，设置为PROCESSING
        - 开始回复时，设置为RESPONDING
        - 回复完成后，必须设置为IDLE
        
        注意：外部服务必须正确管理状态生命周期，
             处理完成后必须重置为IDLE，包括异常情况。
        """
        # 物理事实优先：用户正在说话时，拒绝外部强制切换状态
        # 这防止了状态劫持导致的状态不同步
        if self.current_state == SystemState.COLLECTING:
            logger.warning(
                f"拒绝状态切换 {self.current_state.value} -> {state.value}: "
                "用户正在说话"
            )
            return

        self._transition_to(state)
    
    def get_state(self) -> SystemState:
        """获取当前状态"""
        return self.current_state
    
    def reset(self) -> None:
        """重置状态"""
        self.current_state = SystemState.IDLE
        self.state_start_time = time.time() * 1000
        self.last_speech_end_time = 0.0
        self.interruption_count = 0
        logger.info("InterruptionManager已重置")
    
    def get_stats(self) -> dict:
        """获取打断统计信息"""
        return {
            "current_state": self.current_state.value,
            "interruption_count": self.interruption_count,
            "state_duration_ms": time.time() * 1000 - self.state_start_time
        }
    
    def _transition_to(self, new_state: SystemState) -> None:
        """状态转换（内部方法）"""
        if self.current_state == new_state:
            return
        
        old_state = self.current_state
        self.current_state = new_state
        self.state_start_time = time.time() * 1000  # ms
        
        logger.info(f"状态转换: {old_state.value} → {new_state.value}")


__all__ = ["InterruptionManager"]
