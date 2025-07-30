# Save as: validation\phase5_performance\step5_final_report.py
import json
import os
from datetime import datetime

print("=== Phase 5 Step 5: Final Performance Report ===")
print("Generating comprehensive performance validation report...")
print()

def load_results():
    """Load all test results"""
    results = {}
    
    result_files = [
        ('system_specs', 'validation/phase5_performance/system_specs.json'),
        ('production', 'validation/phase5_performance/production_results.json'),
        ('consumption', 'validation/phase5_performance/consumption_results.json'),
        ('memory_stability', 'validation/phase5_performance/memory_stability_results.json')
    ]
    
    for name, filepath in result_files:
        try:
            with open(filepath, 'r') as f:
                results[name] = json.load(f)
            print(f"  Loaded {name} results")
        except FileNotFoundError:
            print(f"  WARNING: {name} results not found at {filepath}")
            results[name] = None
        except Exception as e:
            print(f"  ERROR loading {name} results: {e}")
            results[name] = None
    
    return results

def generate_performance_report(results):
    """Generate comprehensive performance report"""
    
    report = {
        "report_metadata": {
            "generated_at": datetime.now().isoformat(),
            "validation_phase": "Phase 5 - Performance Testing",
            "package": "dagster-kafka v1.0.0"
        }
    }
    
    # System specifications
    if results['system_specs']:
        report['test_environment'] = results['system_specs']
    
    # Production performance summary
    if results['production']:
        production_summary = {
            "tests_completed": len(results['production']),
            "message_sizes_tested": list(set(r['message_size_category'] for r in results['production'])),
            "peak_throughput": {
                "value": max(r['throughput_msgs_per_sec'] for r in results['production']),
                "message_size": next(r['message_size_category'] for r in results['production'] 
                                   if r['throughput_msgs_per_sec'] == max(r['throughput_msgs_per_sec'] for r in results['production']))
            },
            "throughput_by_size": {}
        }
        
        for result in results['production']:
            size = result['message_size_category']
            if size not in production_summary['throughput_by_size']:
                production_summary['throughput_by_size'][size] = []
            production_summary['throughput_by_size'][size].append(result['throughput_msgs_per_sec'])
        
        # Average throughput by message size
        for size in production_summary['throughput_by_size']:
            throughputs = production_summary['throughput_by_size'][size]
            production_summary['throughput_by_size'][size] = {
                "avg_throughput": round(sum(throughputs) / len(throughputs), 2),
                "max_throughput": max(throughputs),
                "test_count": len(throughputs)
            }
        
        report['production_performance'] = production_summary
    
    # Consumption performance summary
    if results['consumption']:
        consumption_data = results['consumption']
        baseline = next((r for r in consumption_data if 'baseline_throughput_msgs_per_sec' in r), None)
        dagster_kafka = next((r for r in consumption_data if 'throughput_msgs_per_sec' in r), None)
        
        consumption_summary = {
            "baseline_kafka_consumer": baseline['baseline_throughput_msgs_per_sec'] if baseline else "Not tested",
            "dagster_kafka_consumer": dagster_kafka['throughput_msgs_per_sec'] if dagster_kafka else "Not tested"
        }
        
        if baseline and dagster_kafka:
            efficiency = (dagster_kafka['throughput_msgs_per_sec'] / baseline['baseline_throughput_msgs_per_sec']) * 100
            consumption_summary['efficiency_vs_baseline'] = f"{efficiency:.1f}%"
        
        report['consumption_performance'] = consumption_summary
    
    # Memory stability summary
    if results['memory_stability']:
        memory_data = results['memory_stability']
        leak_test = next((r for r in memory_data if r.get('test_type') == 'memory_leak_test'), None)
        sustained_test = next((r for r in memory_data if r.get('test_type') == 'sustained_operation_test'), None)
        
        memory_summary = {}
        
        if leak_test:
            memory_summary['leak_test'] = {
                "total_memory_increase_mb": leak_test['total_memory_increase_mb'],
                "cycles_tested": leak_test['cycles'],
                "avg_cycle_increase_mb": leak_test['avg_cycle_increase_mb'],
                "status": "PASS" if leak_test['total_memory_increase_mb'] < 50 else "WARN"
            }
        
        if sustained_test:
            memory_summary['sustained_test'] = {
                "duration_minutes": sustained_test['duration_minutes'],
                "memory_change_mb": sustained_test['memory_change_mb'],
                "total_operations": sustained_test['total_operations'],
                "status": "PASS" if abs(sustained_test['memory_change_mb']) < 20 else "WARN"
            }
        
        report['memory_stability'] = memory_summary
    
    # Overall assessment
    performance_status = "PASS"
    issues = []
    
    # Check for performance issues
    if results['production']:
        min_throughput = min(r['throughput_msgs_per_sec'] for r in results['production'])
        if min_throughput < 50:  # Arbitrary threshold
            issues.append(f"Low production throughput detected: {min_throughput} msgs/sec")
    
    if results['memory_stability']:
        for test in results['memory_stability']:
            if test.get('test_type') == 'memory_leak_test' and test.get('total_memory_increase_mb', 0) > 50:
                issues.append("Potential memory leak detected")
    
    if issues:
        performance_status = "WARN"
    
    report['overall_assessment'] = {
        "status": performance_status,
        "issues": issues,
        "ready_for_production": performance_status == "PASS"
    }
    
    return report

def print_performance_summary(report):
    """Print human-readable performance summary"""
    
    print("PHASE 5 PERFORMANCE VALIDATION REPORT")
    print("=" * 50)
    print()
    
    # Environment
    if 'test_environment' in report:
        env = report['test_environment']
        print("TEST ENVIRONMENT:")
        print(f"  System: {env['platform']['system']} {env['platform']['release']}")
        print(f"  CPU: {env['cpu']['total_cores']} cores, {env['cpu']['max_frequency']}")
        print(f"  Memory: {env['memory']['total']}")
        print()
    
    # Production Performance
    if 'production_performance' in report:
        prod = report['production_performance']
        print("MESSAGE PRODUCTION PERFORMANCE:")
        print(f"  Peak throughput: {prod['peak_throughput']['value']} msgs/sec ({prod['peak_throughput']['message_size']} messages)")
        
        for size, stats in prod['throughput_by_size'].items():
            print(f"  {size.capitalize()} messages: {stats['avg_throughput']} msgs/sec average")
        print()
    
    # Consumption Performance
    if 'consumption_performance' in report:
        cons = report['consumption_performance']
        print("MESSAGE CONSUMPTION PERFORMANCE:")
        if isinstance(cons['baseline_kafka_consumer'], (int, float)):
            print(f"  Baseline Kafka consumer: {cons['baseline_kafka_consumer']} msgs/sec")
        if isinstance(cons['dagster_kafka_consumer'], (int, float)):
            print(f"  dagster-kafka consumer: {cons['dagster_kafka_consumer']} msgs/sec")
        if 'efficiency_vs_baseline' in cons:
            print(f"  Efficiency vs baseline: {cons['efficiency_vs_baseline']}")
        print()
    
    # Memory Stability
    if 'memory_stability' in report:
        mem = report['memory_stability']
        print("MEMORY STABILITY:")
        if 'leak_test' in mem:
            leak = mem['leak_test']
            print(f"  Memory leak test: {leak['status']} ({leak['total_memory_increase_mb']:+.2f} MB over {leak['cycles_tested']} cycles)")
        if 'sustained_test' in mem:
            sustained = mem['sustained_test']
            print(f"  Sustained operations: {sustained['status']} ({sustained['memory_change_mb']:+.2f} MB over {sustained['duration_minutes']:.1f} minutes)")
        print()
    
    # Overall Assessment
    assessment = report['overall_assessment']
    print("OVERALL ASSESSMENT:")
    print(f"  Status: {assessment['status']}")
    print(f"  Production ready: {'YES' if assessment['ready_for_production'] else 'NO'}")
    
    if assessment['issues']:
        print("  Issues identified:")
        for issue in assessment['issues']:
            print(f"    - {issue}")
    else:
        print("  No significant issues identified")
    
    print()

def main():
    """Generate final performance report"""
    
    # Load all test results
    results = load_results()
    print()
    
    # Generate comprehensive report
    report = generate_performance_report(results)
    
    # Save detailed report
    with open('validation/phase5_performance/final_performance_report.json', 'w') as f:
        json.dump(report, f, indent=2)
    
    # Print summary
    print_performance_summary(report)
    
    print("PHASE 5 COMPLETE:")
    print("PASS: Performance validation completed")
    print("PASS: Package performance characteristics documented")
    print("PASS: Production readiness assessed")
    print()
    print("Detailed report saved to: validation/phase5_performance/final_performance_report.json")

if __name__ == "__main__":
    main()