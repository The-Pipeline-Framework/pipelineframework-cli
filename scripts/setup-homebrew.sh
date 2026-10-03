#!/bin/sh
set -eu
if command -v brew >/dev/null 2>&1; then
  brew --version
elif [ "$(uname -s)" = Linux ]; then
  prefix=/home/linuxbrew/.linuxbrew
  if [ ! -x "$prefix/bin/brew" ]; then
    if [ ! -x "$prefix/Homebrew/bin/brew" ]; then
      sudo mkdir -p "$prefix"
      sudo chown "$(id -un):$(id -gn)" "$prefix"
      git clone https://github.com/Homebrew/brew "$prefix/Homebrew"
      git -C "$prefix/Homebrew" checkout 67984c752d13f3bbcb9aba059d727930aec887dc
    fi
    mkdir -p "$prefix/bin"
    ln -s ../Homebrew/bin/brew "$prefix/bin/brew"
  fi
  "$prefix/bin/brew" --version
  printf '%s\n' "$prefix/bin" >> "$GITHUB_PATH"
else
  echo 'Homebrew must be installed on this macOS runner' >&2
  exit 1
fi
