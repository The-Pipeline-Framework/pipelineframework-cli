#!/bin/sh
set -eu
if command -v brew >/dev/null 2>&1; then
  brew --version
elif [ "$(uname -s)" = Linux ]; then
  prefix=/home/linuxbrew/.linuxbrew
  sudo mkdir -p "$prefix"
  sudo chown "$(id -un):$(id -gn)" "$prefix"
  git clone https://github.com/Homebrew/brew "$prefix/Homebrew"
  git -C "$prefix/Homebrew" checkout 67984c752d13f3bbcb9aba059d727930aec887dc
  mkdir -p "$prefix/bin"
  ln -s ../Homebrew/bin/brew "$prefix/bin/brew"
  printf '%s\n' "$prefix/bin" >> "$GITHUB_PATH"
else
  echo 'Homebrew must be installed on this macOS runner' >&2
  exit 1
fi
