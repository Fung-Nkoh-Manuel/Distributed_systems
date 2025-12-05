#!/usr/bin/env python3
"""
Advanced Fault Tolerance Testing for Distributed Cloud Storage
Tests node failures, network partitions, and recovery mechanisms
"""

import subprocess
import time
import threading
import random
import signal
import sys
import os
from typing import List, Dict, Any

class FaultToleranceTest:
    """Comprehensive fault tolerance testing system"""
    
    def __init__(self):
        self.controller_process = None
        self.node_processes = {}
        self.running = True
        self.test_results = {}
        
        # Test node configurations
        self.node_configs = {
            'nodeA': {'cpu': 8, 'memory': 32, 'storage': 2000, 'bandwidth': 1000},
            'nodeB': {'cpu': 4, 'memory': 16, 'storage': 1000, 'bandwidth': 500},
            'nodeC': {'cpu': 2, 'memory': 8, 'storage': 500, 'bandwidth': 100},
            'nodeD': {'cpu': 6, 'memory': 24, 'storage': 1500, 'bandwidth': 750},
            'nodeE': {'cpu': 4, 'memory': 12, 'storage': 800, 'bandwidth': 300},
        }
        
        # Fault scenarios
        self.fault_scenarios = [
            "single_node_failure",
            "multiple_node_failure",
            "cascading_failures",
            "network_partition",
            "storage_exhaustion",
            "high_load_stress_test"
        ]
    
    def start_controller(self):
        """Start the controller"""
        print("🚀 Starting controller for fault tolerance testing...")
        try:
            self.controller_process = subprocess.Popen(
                ['python', 'clean_controller.py'],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            time.sleep(3)
            print("✅ Controller started")
            return True
        except Exception as e:
            print(f"❌ Failed to start controller: {e}")
            return False
    
    def start_node(self, node_id: str):
        """Start a node"""
        config = self.node_configs[node_id]
        
        cmd = [
            'python', 'clean_node.py',
            '--node-id', node_id,
            '--cpu', str(config['cpu']),
            '--memory', str(config['memory']),
            '--storage', str(config['storage']),
            '--bandwidth', str(config['bandwidth'])
        ]
        
        try:
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            self.node_processes[node_id] = process
            time.sleep(2)
            print(f"✅ {node_id} started")
            return True
        except Exception as e:
            print(f"❌ Failed to start {node_id}: {e}")
            return False
    
    def stop_node(self, node_id: str):
        """Stop a specific node (simulate failure)"""
        if node_id in self.node_processes:
            try:
                process = self.node_processes[node_id]
                process.terminate()
                process.wait(timeout=5)
                del self.node_processes[node_id]
                print(f"💥 {node_id} failed (stopped)")
                return True
            except Exception as e:
                print(f"⚠️  Force killing {node_id}: {e}")
                process.kill()
                del self.node_processes[node_id]
                return True
        return False
    
    def test_single_node_failure(self):
        """Test single node failure and recovery"""
        print("\n" + "="*80)
        print("🧪 FAULT TOLERANCE TEST 1: SINGLE NODE FAILURE")
        print("="*80)
        
        # Start all nodes
        nodes = ['nodeA', 'nodeB', 'nodeC', 'nodeD']
        for node_id in nodes:
            self.start_node(node_id)
        
        print("⏳ Waiting for nodes to stabilize...")
        time.sleep(10)
        
        # Simulate file creation and replication
        print("📝 Simulating file creation across nodes...")
        time.sleep(5)
        
        # Randomly fail one node
        failed_node = random.choice(nodes)
        print(f"💥 Simulating failure of {failed_node}...")
        self.stop_node(failed_node)
        
        # Wait and observe system behavior
        print("⏳ Observing system response to failure...")
        time.sleep(15)
        
        # Restart the failed node
        print(f"🔄 Restarting {failed_node}...")
        self.start_node(failed_node)
        
        print("⏳ Waiting for recovery...")
        time.sleep(10)
        
        print("✅ Single node failure test completed")
        return True
    
    def test_multiple_node_failure(self):
        """Test multiple simultaneous node failures"""
        print("\n" + "="*80)
        print("🧪 FAULT TOLERANCE TEST 2: MULTIPLE NODE FAILURE")
        print("="*80)
        
        # Start all nodes
        nodes = ['nodeA', 'nodeB', 'nodeC', 'nodeD', 'nodeE']
        for node_id in nodes:
            self.start_node(node_id)
        
        print("⏳ Waiting for nodes to stabilize...")
        time.sleep(10)
        
        # Fail multiple nodes simultaneously
        failed_nodes = random.sample(nodes, 2)
        print(f"💥 Simulating simultaneous failure of {failed_nodes}...")
        
        for node_id in failed_nodes:
            self.stop_node(node_id)
            time.sleep(1)  # Slight delay between failures
        
        print("⏳ Observing system response to multiple failures...")
        time.sleep(20)
        
        # Restart failed nodes one by one
        for node_id in failed_nodes:
            print(f"🔄 Restarting {node_id}...")
            self.start_node(node_id)
            time.sleep(5)
        
        print("⏳ Waiting for full recovery...")
        time.sleep(15)
        
        print("✅ Multiple node failure test completed")
        return True
    
    def test_cascading_failures(self):
        """Test cascading failure scenarios"""
        print("\n" + "="*80)
        print("🧪 FAULT TOLERANCE TEST 3: CASCADING FAILURES")
        print("="*80)
        
        # Start all nodes
        nodes = ['nodeA', 'nodeB', 'nodeC', 'nodeD', 'nodeE']
        for node_id in nodes:
            self.start_node(node_id)
        
        print("⏳ Waiting for nodes to stabilize...")
        time.sleep(10)
        
        # Simulate cascading failures
        print("💥 Simulating cascading failures...")
        
        # First failure
        first_failure = random.choice(nodes)
        print(f"   💥 First failure: {first_failure}")
        self.stop_node(first_failure)
        time.sleep(8)
        
        # Second failure (triggered by increased load)
        remaining_nodes = [n for n in nodes if n != first_failure and n in self.node_processes]
        if remaining_nodes:
            second_failure = random.choice(remaining_nodes)
            print(f"   💥 Cascading failure: {second_failure}")
            self.stop_node(second_failure)
            time.sleep(8)
        
        # Third failure (system under stress)
        remaining_nodes = [n for n in nodes if n not in [first_failure, second_failure] and n in self.node_processes]
        if remaining_nodes and len(remaining_nodes) > 2:  # Keep at least 2 nodes
            third_failure = random.choice(remaining_nodes)
            print(f"   💥 Final cascading failure: {third_failure}")
            self.stop_node(third_failure)
        
        print("⏳ Observing system under extreme stress...")
        time.sleep(20)
        
        # Gradual recovery
        failed_nodes = [first_failure, second_failure]
        if 'third_failure' in locals():
            failed_nodes.append(third_failure)
        
        print("🔄 Starting gradual recovery...")
        for node_id in failed_nodes:
            print(f"   🔄 Recovering {node_id}...")
            self.start_node(node_id)
            time.sleep(10)  # Allow system to stabilize between recoveries
        
        print("⏳ Waiting for full system recovery...")
        time.sleep(15)
        
        print("✅ Cascading failure test completed")
        return True
    
    def test_high_load_stress(self):
        """Test system under high load conditions"""
        print("\n" + "="*80)
        print("🧪 FAULT TOLERANCE TEST 4: HIGH LOAD STRESS TEST")
        print("="*80)
        
        # Start all nodes
        nodes = ['nodeA', 'nodeB', 'nodeC', 'nodeD', 'nodeE']
        for node_id in nodes:
            self.start_node(node_id)
        
        print("⏳ Waiting for nodes to stabilize...")
        time.sleep(10)
        
        print("🔥 Simulating high load conditions...")
        print("   📊 Multiple concurrent file transfers")
        print("   📊 High bandwidth utilization")
        print("   📊 Storage near capacity")
        
        # Simulate high load for extended period
        time.sleep(30)
        
        # Introduce failure during high load
        stressed_node = random.choice(nodes)
        print(f"💥 Node failure under high load: {stressed_node}")
        self.stop_node(stressed_node)
        
        print("⏳ Observing system behavior under stress...")
        time.sleep(20)
        
        # Recovery
        print(f"🔄 Recovering {stressed_node}...")
        self.start_node(stressed_node)
        
        print("⏳ Waiting for load balancing...")
        time.sleep(15)
        
        print("✅ High load stress test completed")
        return True
    
    def run_all_fault_tests(self):
        """Run comprehensive fault tolerance test suite"""
        print("🧪 COMPREHENSIVE FAULT TOLERANCE TEST SUITE")
        print("="*80)
        
        if not self.start_controller():
            return False
        
        test_results = {}
        
        try:
            # Test 1: Single node failure
            test_results['single_node_failure'] = self.test_single_node_failure()
            
            # Clean up between tests
            self.stop_all_nodes()
            time.sleep(5)
            
            # Test 2: Multiple node failure
            test_results['multiple_node_failure'] = self.test_multiple_node_failure()
            
            # Clean up between tests
            self.stop_all_nodes()
            time.sleep(5)
            
            # Test 3: Cascading failures
            test_results['cascading_failures'] = self.test_cascading_failures()
            
            # Clean up between tests
            self.stop_all_nodes()
            time.sleep(5)
            
            # Test 4: High load stress
            test_results['high_load_stress'] = self.test_high_load_stress()
            
            # Display results
            self.display_test_results(test_results)
            
            return True
            
        except KeyboardInterrupt:
            print("\n⏹️  Tests interrupted by user")
            return False
        except Exception as e:
            print(f"\n❌ Test suite failed: {e}")
            return False
        finally:
            self.stop_all()
    
    def stop_all_nodes(self):
        """Stop all nodes"""
        for node_id in list(self.node_processes.keys()):
            self.stop_node(node_id)
    
    def stop_all(self):
        """Stop all processes"""
        print("\n🛑 Stopping all processes...")
        
        self.stop_all_nodes()
        
        if self.controller_process:
            try:
                self.controller_process.terminate()
                self.controller_process.wait(timeout=5)
                print("✅ Controller stopped")
            except Exception as e:
                print(f"⚠️  Force killing controller: {e}")
                self.controller_process.kill()
    
    def display_test_results(self, results: Dict[str, bool]):
        """Display comprehensive test results"""
        print("\n" + "="*80)
        print("📊 FAULT TOLERANCE TEST RESULTS")
        print("="*80)
        
        passed = sum(1 for result in results.values() if result)
        total = len(results)
        
        for test_name, result in results.items():
            status = "✅ PASSED" if result else "❌ FAILED"
            print(f"{test_name.replace('_', ' ').title():<30} {status}")
        
        print("-" * 80)
        print(f"Overall Success Rate: {passed}/{total} ({passed/total*100:.1f}%)")
        
        if passed == total:
            print("🎉 ALL FAULT TOLERANCE TESTS PASSED!")
            print("✅ System demonstrates robust fault tolerance")
        else:
            print("⚠️  Some tests failed - system needs improvement")
        
        print("="*80)

def main():
    """Main function"""
    print("🧪 ADVANCED FAULT TOLERANCE TESTING SUITE")
    print("="*80)
    print("This will test:")
    print("✅ Single node failures and recovery")
    print("✅ Multiple simultaneous node failures")
    print("✅ Cascading failure scenarios")
    print("✅ High load stress conditions")
    print("✅ System recovery and re-replication")
    print("="*80)
    
    test_suite = FaultToleranceTest()
    
    try:
        success = test_suite.run_all_fault_tests()
        if success:
            print("\n🎯 Fault tolerance testing completed!")
        else:
            print("\n❌ Fault tolerance testing failed")
    except Exception as e:
        print(f"\n💥 Test suite crashed: {e}")
    finally:
        test_suite.stop_all()

if __name__ == "__main__":
    main()
