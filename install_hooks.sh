#!/bin/bash

HOOKS_DIR=".git/hooks"
HOOKS_SRC_DIR="hooks"

# Check if .git exists
if [ ! -d "$HOOKS_DIR" ]; then
  echo "This script must be run from the root of a Git repository."
  exit 1
fi

# Install all hooks from hooks directory
for hook in "$HOOKS_SRC_DIR"/*; do
  hook_name=$(basename "$hook")
  
  # Copy the hook to the .git/hooks directory
  cp "$hook" "$HOOKS_DIR/$hook_name"
  
  # Make the hook executable
  chmod +x "$HOOKS_DIR/$hook_name"
  
  echo "Installed $hook_name hook."
done

echo "All available hooks have been installed successfully."
