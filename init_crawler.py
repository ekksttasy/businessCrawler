#!/usr/bin/env python3
"""
Initialization script for the business crawler
Can be called from parent repo or run standalone
"""

import os
import sys
from pathlib import Path


def check_dependencies():
    """Check if required dependencies are installed"""
    required = [
        'aiohttp',
        'mysql.connector',
        'dotenv',
        'schedule'
    ]
    
    missing = []
    for package in required:
        try:
            if package == 'mysql.connector':
                import mysql.connector
            elif package == 'dotenv':
                from dotenv import load_dotenv
            else:
                __import__(package)
        except ImportError:
            missing.append(package)
    
    if missing:
        print("❌ Missing dependencies:")
        for pkg in missing:
            print(f"   - {pkg}")
        print("\n📦 Install with: pip install -r requirements.txt")
        return False
    
    print("✅ All dependencies installed")
    return True


def check_config():
    """Check if .env file exists with required config"""
    env_locations = [
        '.env',
        '../.env',
        '../../.env',
    ]
    
    env_file = None
    for location in env_locations:
        if os.path.exists(location):
            env_file = location
            break
    
    if not env_file:
        print("⚠️  No .env file found!")
        print("Create one from .env.template:")
        print("   cp .env.template .env")
        print("   # Then edit .env with your credentials")
        return False
    
    print(f"✅ Found config at: {env_file}")
    
    # Check for required variables
    from dotenv import load_dotenv
    load_dotenv(env_file)
    
    required_vars = ['DB_HOST', 'DB_USER', 'DB_PASSWORD', 'DB_NAME']
    missing_vars = []
    
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        print("⚠️  Missing required variables in .env:")
        for var in missing_vars:
            print(f"   - {var}")
        return False
    
    print("✅ All required config variables present")
    return True


def test_db_connection():
    """Test MySQL database connection"""
    try:
        import mysql.connector
        from dotenv import load_dotenv
        
        # Load config
        for env_path in ['.env', '../.env', '../../.env']:
            if os.path.exists(env_path):
                load_dotenv(env_path)
                break
        
        connection = mysql.connector.connect(
            host=os.getenv('DB_HOST'),
            port=int(os.getenv('DB_PORT', 3306)),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD')
        )
        
        if connection.is_connected():
            print("✅ Database connection successful")
            connection.close()
            return True
        
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False
    
    return False


def main():
    """Run initialization checks"""
    print("=" * 50)
    print("Business Crawler - Initialization Check")
    print("=" * 50)
    print()
    
    checks = [
        ("Dependencies", check_dependencies),
        ("Configuration", check_config),
        ("Database Connection", test_db_connection)
    ]
    
    all_passed = True
    for check_name, check_func in checks:
        print(f"\n🔍 Checking {check_name}...")
        if not check_func():
            all_passed = False
            print(f"❌ {check_name} check failed\n")
        else:
            print(f"✅ {check_name} check passed\n")
    
    print("=" * 50)
    if all_passed:
        print("🎉 All checks passed! Crawler is ready to run.")
        print("\nRun the crawler with:")
        print("   python business_crawler.py")
    else:
        print("⚠️  Some checks failed. Fix the issues above before running.")
        sys.exit(1)
    print("=" * 50)


if __name__ == "__main__":
    main()
