#!/usr/bin/env python3
"""
Slack Integration Test Script for OneMap Building Workflow

This script tests the Slack webhook integration with sample data
to ensure notifications work correctly before running the full workflow.
"""

import json
import requests
import os
import sys
from datetime import datetime
import argparse

def create_test_payload(test_scenario="success"):
    """Create test Slack payload based on different scenarios"""
    
    current_date = datetime.now().strftime("%d/%m/%Y")
    
    if test_scenario == "success":
        # Successful processing with changes
        payload = {
            "text": "🏢 OneMap Building Data Processing Complete (TEST)",
            "attachments": [
                {
                    "color": "warning",
                    "title": "⚠️ OneMap Building Processing - Minor Changes (TEST DATA)",
                    "fields": [
                        {
                            "title": "📅 Processing Period",
                            "value": f"*15/04/2025* → *{current_date}*",
                            "short": False
                        },
                        {
                            "title": "📊 Total Processed",
                            "value": "142,500",
                            "short": True
                        },
                        {
                            "title": "🏗️ Excluded (Construction)",
                            "value": "1,250 (0.9%)",
                            "short": True
                        },
                        {
                            "title": "🏢 Final Buildings",
                            "value": "141,250",
                            "short": True
                        },
                        {
                            "title": "🔄 Duplicates Removed",
                            "value": "450",
                            "short": True
                        },
                        {
                            "title": "🏠 Residential",
                            "value": "105,000",
                            "short": True
                        },
                        {
                            "title": "🏢 Non-Residential",
                            "value": "36,250",
                            "short": True
                        }
                    ],
                    "actions": [
                        {
                            "type": "button",
                            "text": "📊 Main Dataset (141,250)",
                            "url": "https://github.com/your-repo/onemap-building-sg/raw/main/data/correction_differences_onemap_15052025.csv",
                            "style": "primary"
                        },
                        {
                            "type": "button",
                            "text": "🆕 New Buildings (25)",
                            "url": "https://github.com/your-repo/onemap-building-sg/raw/main/data/new_buildings_15_05_2025.csv",
                            "style": "good"
                        },
                        {
                            "type": "button",
                            "text": "📋 All Changes (177)",
                            "url": "https://github.com/your-repo/onemap-building-sg/raw/main/data/building_changes_summary_15_05_2025.csv",
                            "style": "default"
                        },
                        {
                            "type": "button",
                            "text": "📈 Statistics Report",
                            "url": "https://github.com/your-repo/onemap-building-sg/raw/main/data/processing_statistics_15_05_2025.csv",
                            "style": "default"
                        },
                        {
                            "type": "button",
                            "text": "🚫 Excluded (1,250)",
                            "url": "https://github.com/your-repo/onemap-building-sg/raw/main/data/correction_differences_onemap_15052025_excluded.csv",
                            "style": "default"
                        },
                        {
                            "type": "button",
                            "text": "📂 Repository",
                            "url": "https://github.com/your-repo/onemap-building-sg",
                            "style": "default"
                        }
                    ],
                    "footer": "OneMap SG Building Collection | Enhanced with construction filtering",
                    "ts": int(datetime.now().timestamp())
                },
                {
                    "color": "#3498db",
                    "title": "📋 Changes Detected",
                    "fields": [
                        {
                            "title": "🆕 New Buildings",
                            "value": "25",
                            "short": True
                        },
                        {
                            "title": "✏️ Name Changes",
                            "value": "142",
                            "short": True
                        },
                        {
                            "title": "📍 Location Changes",
                            "value": "10",
                            "short": True
                        },
                        {
                            "title": "📊 Total Changes",
                            "value": "177",
                            "short": True
                        }
                    ]
                },
                {
                    "color": "#2ecc71",
                    "title": "🆕 New Buildings Sample",
                    "text": "`018501` Tampines Exchange - Tampines Central 1\n`520101` Block 101 Toa Payoh Lorong 1\n`730456` Woodlands Regional Centre - Woodlands Ave 2\n`440567` Block 567 Clementi Ave 3\n`160789` Block 789 Bukit Merah Central\n... and 20 more",
                    "footer": "Click 'New Buildings' button above to download complete list"
                },
                {
                    "color": "#e74c3c",
                    "title": "🚫 Construction/Temporary Buildings Excluded",
                    "fields": [
                        {
                            "title": "Excluded Count",
                            "value": "1,250 buildings (0.9%)",
                            "short": True
                        },
                        {
                            "title": "Exclusion Criteria",
                            "value": "• Under construction (u/c)\n• Temporary site offices\n• Construction structures\n• Sales offices/galleries",
                            "short": True
                        },
                        {
                            "title": "Examples Excluded",
                            "value": "`123456` Construction Site Office\n`789012` Temporary Sales Gallery\n`345678` Building Under Construction",
                            "short": False
                        }
                    ]
                },
                {
                    "color": "#9b59b6",
                    "title": "📊 Download Information",
                    "text": "📥 **Available Downloads:**\n• **Main Dataset**: 141,250 processed buildings\n• **New Buildings**: 25 newly discovered\n• **Changes Summary**: 177 modifications\n• **Excluded Buildings**: 1,250 filtered out\n• **Statistics Report**: Processing metrics\n• **Summary Report**: Human-readable overview",
                    "footer": "All files are automatically generated and ready for download"
                }
            ]
        }
    
    elif test_scenario == "no_changes":
        # No changes detected
        payload = {
            "text": "🏢 OneMap Building Data Processing Complete (TEST)",
            "attachments": [
                {
                    "color": "good",
                    "title": "✅ OneMap Building Processing - No Changes Detected (TEST DATA)",
                    "fields": [
                        {
                            "title": "📅 Processing Period", 
                            "value": f"*15/04/2025* → *{current_date}*",
                            "short": False
                        },
                        {
                            "title": "📊 Total Processed",
                            "value": "142,042",
                            "short": True
                        },
                        {
                            "title": "🏗️ Excluded (Construction)",
                            "value": "892 (0.6%)",
                            "short": True
                        },
                        {
                            "title": "🏢 Final Buildings", 
                            "value": "141,150",
                            "short": True
                        },
                        {
                            "title": "🔄 Duplicates Removed",
                            "value": "325",
                            "short": True
                        }
                    ],
                    "actions": [
                        {
                            "type": "button",
                            "text": "📊 Complete Dataset (141,150)",
                            "url": "https://github.com/your-repo/onemap-building-sg/raw/main/data/correction_differences_onemap_15052025.csv",
                            "style": "primary"
                        },
                        {
                            "type": "button",
                            "text": "📂 Repository",
                            "url": "https://github.com/your-repo/onemap-building-sg",
                            "style": "default"
                        }
                    ],
                    "footer": "OneMap SG Building Collection | Enhanced with construction filtering"
                }
            ]
        }
    
    elif test_scenario == "failure":
        # Processing failure
        payload = {
            "text": "❌ OneMap Building Processing Failed (TEST)",
            "attachments": [
                {
                    "color": "danger",
                    "title": "🚨 Enhanced Workflow Failure (TEST DATA)",
                    "text": "The enhanced OneMap building data processing workflow has failed. This includes download, comparison, correction with construction filtering, and Slack integration.",
                    "fields": [
                        {
                            "title": "Failure Time",
                            "value": datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC'),
                            "short": True
                        },
                        {
                            "title": "Scheduled Run",
                            "value": "15th of each month",
                            "short": True
                        }
                    ],
                    "actions": [
                        {
                            "type": "button",
                            "text": "🔍 Check Workflow Logs",
                            "url": "https://github.com/your-repo/onemap-building-sg/actions",
                            "style": "danger"
                        },
                        {
                            "type": "button",
                            "text": "📂 View Repository",
                            "url": "https://github.com/your-repo/onemap-building-sg",
                            "style": "default"
                        }
                    ]
                }
            ]
        }
    
    return payload

def test_slack_webhook(webhook_url, scenario="success"):
    """Test the Slack webhook with sample data"""
    
    print(f"🧪 Testing Slack webhook with '{scenario}' scenario...")
    
    # Create test payload
    payload = create_test_payload(scenario)
    
    # Save payload to file for inspection
    payload_file = f"test_slack_payload_{scenario}.json"
    with open(payload_file, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    
    print(f"📄 Test payload saved to: {payload_file}")
    print(f"📏 Payload size: {len(json.dumps(payload))} characters")
    print(f"📊 Attachments: {len(payload['attachments'])}")
    
    try:
        # Send to Slack
        print("🚀 Sending test notification to Slack...")
        
        response = requests.post(
            webhook_url,
            json=payload,
            headers={'Content-Type': 'application/json'},
            timeout=30
        )
        
        print(f"📊 Response Status: {response.status_code}")
        print(f"📝 Response Body: {response.text}")
        
        if response.status_code == 200 and response.text == "ok":
            print("✅ Test notification sent successfully!")
            return True
        else:
            print("❌ Test notification failed!")
            print(f"   Status: {response.status_code}")
            print(f"   Response: {response.text}")
            return False
    
    except requests.exceptions.RequestException as e:
        print(f"❌ Network error: {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False

def validate_webhook_url(webhook_url):
    """Basic validation of webhook URL"""
    if not webhook_url:
        print("❌ Webhook URL is empty")
        return False
    
    if not webhook_url.startswith("https://hooks.slack.com/"):
        print("❌ Invalid Slack webhook URL format")
        return False
    
    print("✅ Webhook URL format looks valid")
    return True

def main():
    """Main function for testing Slack integration"""
    parser = argparse.ArgumentParser(description='Test Slack webhook integration for OneMap workflow')
    parser.add_argument('--webhook-url', type=str, 
                        help='Slack webhook URL (or set SLACK_WEBHOOK environment variable)')
    parser.add_argument('--scenario', type=str, default='success',
                        choices=['success', 'no_changes', 'failure'],
                        help='Test scenario to simulate')
    parser.add_argument('--all-scenarios', action='store_true',
                        help='Test all scenarios')
    
    args = parser.parse_args()
    
    # Get webhook URL
    webhook_url = args.webhook_url or os.getenv('SLACK_WEBHOOK')
    
    if not webhook_url:
        print("❌ Slack webhook URL not provided!")
        print("   Use --webhook-url argument or set SLACK_WEBHOOK environment variable")
        sys.exit(1)
    
    if not validate_webhook_url(webhook_url):
        sys.exit(1)
    
    print("=" * 60)
    print("SLACK INTEGRATION TEST FOR ONEMAP WORKFLOW")
    print("=" * 60)
    print(f"🌐 Webhook URL: {webhook_url[:50]}...")
    print(f"📅 Test Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    success_count = 0
    total_tests = 0
    
    if args.all_scenarios:
        scenarios = ['success', 'no_changes', 'failure']
    else:
        scenarios = [args.scenario]
    
    for scenario in scenarios:
        print(f"\n{'='*40}")
        print(f"Testing scenario: {scenario.upper()}")
        print('='*40)
        
        success = test_slack_webhook(webhook_url, scenario)
        
        if success:
            success_count += 1
        total_tests += 1
        
        print(f"Result: {'✅ SUCCESS' if success else '❌ FAILED'}")
        
        if scenario != scenarios[-1]:
            print("\nWaiting 3 seconds before next test...")
            import time
            time.sleep(3)
    
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    print(f"Total Tests: {total_tests}")
    print(f"Successful: {success_count}")
    print(f"Failed: {total_tests - success_count}")
    print(f"Success Rate: {(success_count/total_tests)*100:.1f}%")
    
    if success_count == total_tests:
        print("\n🎉 All tests passed! Slack integration is working correctly.")
        print("✅ Your OneMap workflow is ready for production use.")
    else:
        print(f"\n⚠️ {total_tests - success_count} test(s) failed.")
        print("❌ Please check your Slack webhook configuration.")
    
    print("\n📄 Generated test files:")
    for scenario in scenarios:
        print(f"   • test_slack_payload_{scenario}.json")
    
    print("\n🔧 Next steps:")
    print("   1. Review the generated JSON files")
    print("   2. Check your Slack channel for test messages")
    print("   3. Update the GitHub workflow if needed")
    print("   4. Set up the SLACK_WEBHOOK secret in your repository")

if __name__ == "__main__":
    main()