#!/usr/bin/env python3
"""
打断功能测试脚本

测试Cascade的打断检测功能，包括：
1. InterruptionManager 状态管理测试
2. 打断事件触发测试
3. 状态转换测试
4. 与StreamProcessor集成测试

使用方法：
    python tests/test_interruption.py -v
"""

import asyncio
import time
import unittest
from unittest.mock import MagicMock, patch

import cascade
from cascade import (
    InterruptionManager,
    InterruptionConfig,
    InterruptionEvent,
    SystemState,
)


class TestInterruptionManager(unittest.TestCase):
    """InterruptionManager 单元测试"""

    def setUp(self):
        """测试前初始化"""
        self.config = InterruptionConfig(
            enable_interruption=True,
            min_interval_ms=500
        )
        self.manager = InterruptionManager(self.config)

    def tearDown(self):
        """测试后清理"""
        self.manager.reset()

    def test_initial_state(self):
        """测试初始状态"""
        self.assertEqual(self.manager.get_state(), SystemState.IDLE)
        self.assertEqual(self.manager.interruption_count, 0)

    def test_state_transition_idle_to_collecting(self):
        """测试从IDLE到COLLECTING的状态转换"""
        # 初始状态为IDLE
        self.assertEqual(self.manager.get_state(), SystemState.IDLE)
        
        # 语音开始时，应自动切换到COLLECTING
        result = self.manager.on_speech_start(1000.0)
        
        self.assertIsNone(result)  # IDLE状态下不触发打断
        self.assertEqual(self.manager.get_state(), SystemState.COLLECTING)

    def test_state_transition_collecting_to_idle(self):
        """测试从COLLECTING到IDLE的状态转换"""
        # 先切换到COLLECTING
        self.manager.on_speech_start(1000.0)
        self.assertEqual(self.manager.get_state(), SystemState.COLLECTING)
        
        # 语音结束时，应自动切换回IDLE
        self.manager.on_speech_end(2000.0)
        self.assertEqual(self.manager.get_state(), SystemState.IDLE)

    def test_external_state_setting(self):
        """测试外部设置状态"""
        # 设置为PROCESSING
        self.manager.set_state(SystemState.PROCESSING)
        self.assertEqual(self.manager.get_state(), SystemState.PROCESSING)
        
        # 设置为RESPONDING
        self.manager.set_state(SystemState.RESPONDING)
        self.assertEqual(self.manager.get_state(), SystemState.RESPONDING)
        
        # 设置回IDLE
        self.manager.set_state(SystemState.IDLE)
        self.assertEqual(self.manager.get_state(), SystemState.IDLE)

    def test_reject_state_change_during_collecting(self):
        """测试在COLLECTING状态下拒绝外部状态切换"""
        # 先切换到COLLECTING
        self.manager.on_speech_start(1000.0)
        self.assertEqual(self.manager.get_state(), SystemState.COLLECTING)
        
        # 尝试外部设置状态，应被拒绝
        self.manager.set_state(SystemState.PROCESSING)
        self.assertEqual(self.manager.get_state(), SystemState.COLLECTING)

    def test_interruption_during_processing(self):
        """测试在PROCESSING状态下的打断"""
        # 设置为PROCESSING状态
        self.manager.set_state(SystemState.PROCESSING)
        
        # 设置上次语音结束时间（确保间隔足够）
        self.manager.last_speech_end_time = 0.0
        
        # 语音开始，应触发打断
        result = self.manager.on_speech_start(1000.0)
        
        self.assertIsNotNone(result)
        self.assertIsInstance(result, InterruptionEvent)
        self.assertEqual(result.system_state, SystemState.PROCESSING)
        self.assertEqual(result.event_type, "start_interrupt")
        self.assertEqual(self.manager.interruption_count, 1)
        
        # 打断后应切换到COLLECTING
        self.assertEqual(self.manager.get_state(), SystemState.COLLECTING)

    def test_interruption_during_responding(self):
        """测试在RESPONDING状态下的打断"""
        # 设置为RESPONDING状态
        self.manager.set_state(SystemState.RESPONDING)
        
        # 设置上次语音结束时间
        self.manager.last_speech_end_time = 0.0
        
        # 语音开始，应触发打断
        result = self.manager.on_speech_start(1000.0)
        
        self.assertIsNotNone(result)
        self.assertEqual(result.system_state, SystemState.RESPONDING)
        self.assertEqual(self.manager.interruption_count, 1)

    def test_no_interruption_in_idle(self):
        """测试在IDLE状态下不触发打断"""
        # 初始状态为IDLE
        result = self.manager.on_speech_start(1000.0)
        
        self.assertIsNone(result)
        self.assertEqual(self.manager.interruption_count, 0)

    def test_min_interval_check(self):
        """测试最小间隔检查"""
        # 设置为PROCESSING状态
        self.manager.set_state(SystemState.PROCESSING)
        
        # 设置上次语音结束时间为较近的时间
        self.manager.last_speech_end_time = 800.0  # 800ms
        
        # 语音开始时间为1000ms，间隔只有200ms，小于min_interval_ms(500ms)
        result = self.manager.on_speech_start(1000.0)
        
        self.assertIsNone(result)  # 间隔太短，不触发打断
        self.assertEqual(self.manager.interruption_count, 0)

    def test_interruption_disabled(self):
        """测试禁用打断功能"""
        config = InterruptionConfig(
            enable_interruption=False,
            min_interval_ms=500
        )
        manager = InterruptionManager(config)
        
        # 设置为PROCESSING状态
        manager.set_state(SystemState.PROCESSING)
        manager.last_speech_end_time = 0.0
        
        # 语音开始，不应触发打断
        result = manager.on_speech_start(1000.0)
        
        self.assertIsNone(result)
        self.assertEqual(manager.interruption_count, 0)

    def test_reset(self):
        """测试重置功能"""
        # 进行一些操作
        self.manager.set_state(SystemState.PROCESSING)
        self.manager.last_speech_end_time = 0.0
        self.manager.on_speech_start(1000.0)
        
        # 重置
        self.manager.reset()
        
        self.assertEqual(self.manager.get_state(), SystemState.IDLE)
        self.assertEqual(self.manager.interruption_count, 0)
        self.assertEqual(self.manager.last_speech_end_time, 0.0)

    def test_get_stats(self):
        """测试获取统计信息"""
        # 进行一些操作
        self.manager.set_state(SystemState.PROCESSING)
        self.manager.last_speech_end_time = 0.0
        self.manager.on_speech_start(1000.0)
        
        stats = self.manager.get_stats()
        
        self.assertIn("current_state", stats)
        self.assertIn("interruption_count", stats)
        self.assertIn("state_duration_ms", stats)
        self.assertEqual(stats["interruption_count"], 1)


class TestInterruptionConfig(unittest.TestCase):
    """InterruptionConfig 配置测试"""

    def test_default_config(self):
        """测试默认配置"""
        config = InterruptionConfig()
        
        self.assertTrue(config.enable_interruption)
        self.assertEqual(config.min_interval_ms, 500)

    def test_custom_config(self):
        """测试自定义配置"""
        config = InterruptionConfig(
            enable_interruption=False,
            min_interval_ms=1000
        )
        
        self.assertFalse(config.enable_interruption)
        self.assertEqual(config.min_interval_ms, 1000)

    def test_config_validation(self):
        """测试配置验证"""
        # min_interval_ms 应在 0-5000 之间
        config = InterruptionConfig(min_interval_ms=0)
        self.assertEqual(config.min_interval_ms, 0)
        
        config = InterruptionConfig(min_interval_ms=5000)
        self.assertEqual(config.min_interval_ms, 5000)


class TestInterruptionEvent(unittest.TestCase):
    """InterruptionEvent 事件测试"""

    def test_event_creation(self):
        """测试事件创建"""
        event = InterruptionEvent(
            event_type="start_interrupt",
            timestamp_ms=1000.0,
            system_state=SystemState.PROCESSING,
            confidence=1.0,
            state_duration_ms=500.0
        )
        
        self.assertEqual(event.event_type, "start_interrupt")
        self.assertEqual(event.timestamp_ms, 1000.0)
        self.assertEqual(event.system_state, SystemState.PROCESSING)
        self.assertEqual(event.confidence, 1.0)
        self.assertEqual(event.state_duration_ms, 500.0)


class TestStreamProcessorInterruption(unittest.IsolatedAsyncioTestCase):
    """StreamProcessor 打断功能集成测试"""

    async def test_processor_interruption_config(self):
        """测试处理器打断配置"""
        config = cascade.Config(
            vad_threshold=0.5,
            interruption_config=InterruptionConfig(
                enable_interruption=True,
                min_interval_ms=300
            )
        )
        
        async with cascade.StreamProcessor(config) as processor:
            # 验证打断管理器已初始化
            self.assertIsNotNone(processor.interruption_manager)
            self.assertTrue(processor.interruption_manager.config.enable_interruption)
            self.assertEqual(processor.interruption_manager.config.min_interval_ms, 300)

    async def test_processor_state_management(self):
        """测试处理器状态管理"""
        config = cascade.Config(
            vad_threshold=0.5,
            interruption_config=InterruptionConfig(enable_interruption=True)
        )
        
        async with cascade.StreamProcessor(config) as processor:
            # 初始状态应为IDLE
            self.assertEqual(processor.get_system_state(), SystemState.IDLE)
            
            # 设置为PROCESSING
            processor.set_system_state(SystemState.PROCESSING)
            self.assertEqual(processor.get_system_state(), SystemState.PROCESSING)
            
            # 设置为RESPONDING
            processor.set_system_state(SystemState.RESPONDING)
            self.assertEqual(processor.get_system_state(), SystemState.RESPONDING)
            
            # 设置回IDLE
            processor.set_system_state(SystemState.IDLE)
            self.assertEqual(processor.get_system_state(), SystemState.IDLE)

    async def test_processor_interruption_stats(self):
        """测试处理器打断统计"""
        config = cascade.Config(
            vad_threshold=0.5,
            interruption_config=InterruptionConfig(enable_interruption=True)
        )
        
        async with cascade.StreamProcessor(config) as processor:
            stats = processor.get_interruption_stats()
            
            self.assertIn("current_state", stats)
            self.assertIn("interruption_count", stats)
            self.assertIn("state_duration_ms", stats)


class TestInterruptionScenarios(unittest.TestCase):
    """打断场景测试"""

    def setUp(self):
        """测试前初始化"""
        self.config = InterruptionConfig(
            enable_interruption=True,
            min_interval_ms=500
        )
        self.manager = InterruptionManager(self.config)

    def test_scenario_user_interrupts_llm_processing(self):
        """场景：用户打断LLM处理"""
        # 1. 用户说话完成，系统开始处理
        self.manager.on_speech_start(0.0)
        self.manager.on_speech_end(1000.0)
        self.manager.set_state(SystemState.PROCESSING)
        
        # 2. 用户在处理过程中再次说话（打断）
        result = self.manager.on_speech_start(2000.0)
        
        # 3. 验证打断事件
        self.assertIsNotNone(result)
        self.assertEqual(result.system_state, SystemState.PROCESSING)
        self.assertEqual(self.manager.get_state(), SystemState.COLLECTING)

    def test_scenario_user_interrupts_tts_playback(self):
        """场景：用户打断TTS播放"""
        # 1. 系统正在播放TTS
        self.manager.set_state(SystemState.RESPONDING)
        self.manager.last_speech_end_time = 0.0
        
        # 2. 用户说话打断
        result = self.manager.on_speech_start(1000.0)
        
        # 3. 验证打断事件
        self.assertIsNotNone(result)
        self.assertEqual(result.system_state, SystemState.RESPONDING)

    def test_scenario_rapid_speech_no_false_interrupt(self):
        """场景：快速连续说话不应误触发打断"""
        # 1. 用户说话
        self.manager.on_speech_start(0.0)
        self.manager.on_speech_end(500.0)
        
        # 2. 系统开始处理
        self.manager.set_state(SystemState.PROCESSING)
        
        # 3. 用户很快又说话（间隔小于min_interval_ms）
        result = self.manager.on_speech_start(600.0)  # 间隔只有100ms
        
        # 4. 不应触发打断
        self.assertIsNone(result)

    def test_scenario_complete_conversation_flow(self):
        """场景：完整对话流程"""
        # 1. 初始状态
        self.assertEqual(self.manager.get_state(), SystemState.IDLE)
        
        # 2. 用户开始说话
        self.manager.on_speech_start(0.0)
        self.assertEqual(self.manager.get_state(), SystemState.COLLECTING)
        
        # 3. 用户说话结束
        self.manager.on_speech_end(2000.0)
        self.assertEqual(self.manager.get_state(), SystemState.IDLE)
        
        # 4. 系统开始处理（ASR + LLM）
        self.manager.set_state(SystemState.PROCESSING)
        self.assertEqual(self.manager.get_state(), SystemState.PROCESSING)
        
        # 5. 系统开始回复（TTS）
        self.manager.set_state(SystemState.RESPONDING)
        self.assertEqual(self.manager.get_state(), SystemState.RESPONDING)
        
        # 6. 用户打断
        result = self.manager.on_speech_start(5000.0)
        self.assertIsNotNone(result)
        self.assertEqual(self.manager.get_state(), SystemState.COLLECTING)
        
        # 7. 用户说话结束
        self.manager.on_speech_end(6000.0)
        self.assertEqual(self.manager.get_state(), SystemState.IDLE)

    def test_scenario_multiple_interruptions(self):
        """场景：多次打断"""
        for i in range(3):
            # 设置为RESPONDING状态
            self.manager.set_state(SystemState.RESPONDING)
            self.manager.last_speech_end_time = i * 2000.0
            
            # 触发打断
            result = self.manager.on_speech_start((i + 1) * 2000.0)
            self.assertIsNotNone(result)
            
            # 语音结束
            self.manager.on_speech_end((i + 1) * 2000.0 + 500.0)
        
        # 验证打断计数
        self.assertEqual(self.manager.interruption_count, 3)


def run_tests():
    """运行所有测试"""
    print("🧪 Cascade 打断功能测试")
    print("=" * 60)
    
    # 创建测试套件
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # 添加测试类
    suite.addTests(loader.loadTestsFromTestCase(TestInterruptionManager))
    suite.addTests(loader.loadTestsFromTestCase(TestInterruptionConfig))
    suite.addTests(loader.loadTestsFromTestCase(TestInterruptionEvent))
    suite.addTests(loader.loadTestsFromTestCase(TestStreamProcessorInterruption))
    suite.addTests(loader.loadTestsFromTestCase(TestInterruptionScenarios))
    
    # 运行测试
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # 打印总结
    print("\n" + "=" * 60)
    print("📊 测试总结")
    print(f"   运行测试: {result.testsRun}")
    print(f"   成功: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"   失败: {len(result.failures)}")
    print(f"   错误: {len(result.errors)}")
    
    if result.wasSuccessful():
        print("\n✅ 所有测试通过！")
    else:
        print("\n❌ 部分测试失败")
        
    return result.wasSuccessful()


if __name__ == "__main__":
    import sys
    success = run_tests()
    sys.exit(0 if success else 1)
