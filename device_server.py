import time
import subprocess
import uiautomator2 as u2
import sqlite3
import os
import sys
import json

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from database.device_db import register_device, get_pending_tasks, update_task_status

# Try to import iOS dependencies
try:
    import wda
except ImportError:
    wda = None

def get_connected_devices():
    """Get list of connected Android devices via ADB"""
    try:
        result = subprocess.check_output(["adb", "devices"]).decode("utf-8")
        lines = result.strip().split("\n")[1:]
        devices = []
        for line in lines:
            if "device" in line and "offline" not in line:
                devices.append(line.split("\t")[0])
        return devices
    except Exception as e:
        print(f"Error checking ADB devices: {e}")
        return []

def get_ios_devices():
    """Get list of connected iOS devices via tidevice"""
    try:
        # tidevice list --json
        result = subprocess.check_output(["tidevice", "list", "--json"]).decode("utf-8")
        devices = json.loads(result)
        return [d['udid'] for d in devices]
    except FileNotFoundError:
        # tidevice not installed
        return []
    except Exception as e:
        print(f"Error checking iOS devices: {e}")
        return []

def process_device(serial):
    """Process tasks for a single device"""
    try:
        # 1. Register/Heartbeat
        register_device(serial, platform="Android", status="Online")
        
        # 2. Check for tasks
        tasks = get_pending_tasks(serial)
        if not tasks:
            return

        # 3. Connect to device
        d = u2.connect(serial)
        
        for task in tasks:
            print(f"[{serial}] Processing Task {task['task_id']}: Comment on {task['note_id']}")
            try:
                # Update status to Processing
                update_task_status(task['task_id'], "Processing")
                
                # Wake up device
                d.screen_on()
                d.unlock()
                
                # Open Note via Deep Link
                # xhsdiscover://item/<note_id>
                note_id = task['note_id']
                deep_link = f"xhsdiscover://item/{note_id}"
                
                # Use ADB to open deep link
                subprocess.run(["adb", "-s", serial, "shell", "am", "start", "-a", "android.intent.action.VIEW", "-d", deep_link])
                
                # Wait for app to load
                time.sleep(5)
                
                # Click Comment Input (This selector might need adjustment based on XHS version)
                # Usually the bottom bar input box
                if d(text="说点什么...").exists(timeout=5):
                    d(text="说点什么...").click()
                    time.sleep(1)
                    
                    # Type content
                    d.send_keys(task['content'])
                    
                    # Success
                    update_task_status(task['task_id'], "Dispatched")
                    print(f"[{serial}] Task {task['task_id']} Dispatched (Text entered)")
                else:
                    update_task_status(task['task_id'], "Failed", "Comment input not found")
                    print(f"[{serial}] Task {task['task_id']} Failed: Input not found")
                    
            except Exception as e:
                print(f"[{serial}] Task {task['task_id']} Error: {e}")
                update_task_status(task['task_id'], "Error", str(e))
                
    except Exception as e:
        print(f"Error processing device {serial}: {e}")

def process_ios_device(udid):
    """Process tasks for a single iOS device"""
    if not wda:
        print("Error: 'facebook-wda' not installed. Please run: pip install facebook-wda")
        return

    try:
        # 1. Register/Heartbeat
        register_device(udid, platform="iOS", status="Online")
        
        # 2. Check for tasks
        tasks = get_pending_tasks(udid)
        if not tasks:
            return

        # 3. Connect to device via WDA
        # Assuming WDA is running on port 8100 (default) or forwarded via tidevice
        # For simplicity, we try to connect to the USB-connected device
        c = wda.USBClient(udid)
        
        for task in tasks:
            print(f"[{udid}] Processing iOS Task {task['task_id']}: Comment on {task['note_id']}")
            try:
                # Update status to Processing
                update_task_status(task['task_id'], "Processing")
                
                # Wake up device
                c.unlock()
                
                # Open Note via Deep Link
                # xhsdiscover://item/<note_id>
                note_id = task['note_id']
                deep_link = f"xhsdiscover://item/{note_id}"
                
                # Open URL
                c.session().open_url(deep_link)
                
                # Wait for app to load
                time.sleep(5)
                
                # Click Comment Input
                # iOS selector strategy might differ. Using predicate string or class chain
                # Try to find "说点什么..." static text or text field
                
                # Attempt 1: Find by label/value
                ele = c(label="说点什么...", className="XCUIElementTypeStaticText")
                if not ele.exists:
                     ele = c(value="说点什么...", className="XCUIElementTypeTextField")
                
                if ele.exists:
                    ele.click()
                    time.sleep(1)
                    
                    # Type content
                    c.send_keys(task['content'])
                    
                    # Success
                    update_task_status(task['task_id'], "Dispatched")
                    print(f"[{udid}] Task {task['task_id']} Dispatched (Text entered)")
                else:
                    update_task_status(task['task_id'], "Failed", "Comment input not found")
                    print(f"[{udid}] Task {task['task_id']} Failed: Input not found")
                    
            except Exception as e:
                print(f"[{udid}] Task {task['task_id']} Error: {e}")
                update_task_status(task['task_id'], "Error", str(e))
                
    except Exception as e:
        print(f"Error processing iOS device {udid}: {e}")

def main():
    print("Starting Device Server...")
    print("Monitoring connected devices...")
    
    while True:
        android_devices = get_connected_devices()
        ios_devices = get_ios_devices()
        
        if not android_devices and not ios_devices:
            print("No devices connected. Waiting...", end="\r")
        
        for serial in android_devices:
            process_device(serial)
        
        for udid in ios_devices:
            process_ios_device(udid)
            
        time.sleep(5)

if __name__ == "__main__":
    main()
