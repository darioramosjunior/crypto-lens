"""
Setup script for installing pre-commit hooks
Run: python setup_pre_commit.py
"""

import os
import shutil
import sys
import stat

def setup_pre_commit_hook():
    """Install pre-commit hook in git repository"""
    
    # Get git directory
    git_dir = os.path.join(os.getcwd(), '.git')
    if not os.path.exists(git_dir):
        print("❌ Not a git repository. Please run this script from the root of a git repository.")
        return False
    
    # Get hook directory
    hooks_dir = os.path.join(git_dir, 'hooks')
    if not os.path.exists(hooks_dir):
        os.makedirs(hooks_dir)
    
    # Source and destination paths
    script_dir = os.path.dirname(os.path.abspath(__file__))
    source_hook = os.path.join(script_dir, 'pre-commit')
    dest_hook = os.path.join(hooks_dir, 'pre-commit')
    
    if not os.path.exists(source_hook):
        print(f"❌ Pre-commit script not found at {source_hook}")
        return False
    
    try:
        # Copy the hook
        shutil.copy2(source_hook, dest_hook)
        
        # Make it executable
        st = os.stat(dest_hook)
        os.chmod(dest_hook, st.st_mode | stat.S_IEXEC | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
        
        print(f"✅ Pre-commit hook installed successfully at {dest_hook}")
        print("   Tests will now run automatically before each commit!")
        return True
    
    except Exception as e:
        print(f"❌ Failed to install pre-commit hook: {e}")
        return False

if __name__ == "__main__":
    success = setup_pre_commit_hook()
    sys.exit(0 if success else 1)
