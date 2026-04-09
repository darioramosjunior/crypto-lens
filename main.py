import subprocess
import sys
import os

# Get the directory where main.py is located (app directory)
APP_DIR = os.path.dirname(os.path.abspath(__file__))

def run_script(script_name):
    """Run a Python script and handle errors."""
    # Build absolute path to the script
    script_path = os.path.join(APP_DIR, script_name)
    
    # Verify script exists before attempting to run it
    if not os.path.exists(script_path):
        print(f"\n✗ Script not found: {script_path}")
        return False
    
    print(f"\n{'='*60}")
    print(f"Running: {script_name}")
    print(f"Location: {script_path}")
    print('='*60)
    
    try:
        # Run the script with absolute path
        result = subprocess.run([sys.executable, script_path], check=True, cwd=APP_DIR)
        print(f"\n✓ {script_name} completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"\n✗ {script_name} failed with exit code {e.returncode}")
        return False
    except Exception as e:
        print(f"\n✗ Error running {script_name}: {str(e)}")
        return False

def main():
    """Main function to run all scripts in sequence."""
    scripts = [
        "hourly_fetch_and_pulse.py",
        "daily_fetch_and_pulse.py",
        "oi_change_screener.py"
    ]
    
    print("\n" + "="*60)
    print(f"Starting script execution sequence from: {APP_DIR}")
    print("="*60)
    
    results = {}
    for script in scripts:
        success = run_script(script)
        results[script] = success
        if not success:
            print(f"\nWarning: {script} failed. Continuing with next script...")
    
    # Print summary
    print("\n" + "="*60)
    print("Execution Summary")
    print("="*60)
    for script, success in results.items():
        status = "✓ Success" if success else "✗ Failed"
        print(f"{script}: {status}")
    
    all_success = all(results.values())
    print("\n" + "="*60)
    if all_success:
        print("All scripts completed successfully!")
    else:
        print("Some scripts failed. Check the logs above for details.")
    print("="*60 + "\n")
    
    return 0 if all_success else 1

if __name__ == "__main__":
    sys.exit(main())
