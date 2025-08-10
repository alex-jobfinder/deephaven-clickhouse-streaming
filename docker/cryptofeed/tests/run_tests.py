#!/usr/bin/env python3
"""
Test runner for FeedHandler trades unit tests
"""

import unittest
import sys
import os
import argparse
import time
from io import StringIO

# Add the src directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

def run_unit_tests(verbosity=2, pattern=None):
    """Run unit tests"""
    print("Running unit tests...")
    
    # Discover and run unit tests
    loader = unittest.TestLoader()
    if pattern:
        loader.testNamePatterns = [pattern]
    
    start_dir = os.path.dirname(__file__)
    suite = loader.discover(start_dir, pattern='test_*.py')
    
    runner = unittest.TextTestRunner(verbosity=verbosity)
    result = runner.run(suite)
    
    return result

def run_specific_test(test_name, verbosity=2):
    """Run a specific test"""
    print(f"Running specific test: {test_name}")
    
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromName(test_name)
    
    runner = unittest.TextTestRunner(verbosity=verbosity)
    result = runner.run(suite)
    
    return result

def run_performance_tests():
    """Run performance tests"""
    print("Running performance tests...")
    
    # Import performance test module if it exists
    try:
        from test_performance import TestPerformanceTrades
        suite = unittest.TestLoader().loadTestsFromTestCase(TestPerformanceTrades)
        runner = unittest.TextTestRunner(verbosity=2)
        result = runner.run(suite)
        return result
    except ImportError:
        print("Performance tests not found")
        return None

def generate_test_report(result):
    """Generate a test report"""
    if not result:
        return
    
    print("\n" + "="*50)
    print("TEST REPORT")
    print("="*50)
    
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(f"Skipped: {len(result.skipped) if hasattr(result, 'skipped') else 0}")
    
    if result.failures:
        print("\nFAILURES:")
        for test, traceback in result.failures:
            print(f"  {test}: {traceback}")
    
    if result.errors:
        print("\nERRORS:")
        for test, traceback in result.errors:
            print(f"  {test}: {traceback}")
    
    success_rate = ((result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100) if result.testsRun > 0 else 0
    print(f"\nSuccess rate: {success_rate:.1f}%")
    
    if result.wasSuccessful():
        print("✅ All tests passed!")
    else:
        print("❌ Some tests failed!")

def main():
    """Main test runner function"""
    parser = argparse.ArgumentParser(description='Run FeedHandler trades tests')
    parser.add_argument('--unit', action='store_true', help='Run unit tests')
    parser.add_argument('--integration', action='store_true', help='Run integration tests')
    parser.add_argument('--performance', action='store_true', help='Run performance tests')
    parser.add_argument('--all', action='store_true', help='Run all tests')
    parser.add_argument('--specific', type=str, help='Run specific test by name')
    parser.add_argument('--pattern', type=str, help='Run tests matching pattern')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    parser.add_argument('--report', action='store_true', help='Generate detailed report')
    
    args = parser.parse_args()
    
    verbosity = 2 if args.verbose else 1
    
    # If no specific test type is specified, run all
    if not any([args.unit, args.integration, args.performance, args.all, args.specific]):
        args.all = True
    
    start_time = time.time()
    results = []
    
    try:
        if args.specific:
            result = run_specific_test(args.specific, verbosity)
            results.append(result)
        else:
            if args.unit or args.all:
                result = run_unit_tests(verbosity, args.pattern)
                results.append(result)
            
            if args.integration or args.all:
                # Integration tests are part of the unit test discovery
                pass
            
            if args.performance or args.all:
                result = run_performance_tests()
                if result:
                    results.append(result)
        
        end_time = time.time()
        
        print(f"\nTotal test execution time: {end_time - start_time:.2f} seconds")
        
        if args.report:
            for result in results:
                if result:
                    generate_test_report(result)
        
        # Exit with appropriate code
        all_successful = all(result.wasSuccessful() for result in results if result)
        sys.exit(0 if all_successful else 1)
        
    except KeyboardInterrupt:
        print("\nTest execution interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Error running tests: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()
