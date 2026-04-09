import subprocess
import sys

def run_script(script_name):
    """Run a Python script and handle errors."""
    print(f"\n{'='*60}")
    print(f"Running: {script_name}")
    print('='*60)
    
    try:
        result = subprocess.run([sys.executable, script_name], check=True)
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
    print("Starting script execution sequence")
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
