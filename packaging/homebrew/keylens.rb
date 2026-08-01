# Homebrew formula for keylens.
#
# This file is the template. The real formula lives in the tap repo
# (github.com/keylens/homebrew-tap) as `Formula/keylens.rb`, and the release
# workflow regenerates it with the version and checksums from each release.
#
# Users install with:
#   brew install keylens/tap/keylens
#
# Plain `brew install keylens` needs homebrew-core, which has a notability bar
# (roughly 75+ stars / 30+ forks, or comparable evidence of use). Until then the
# tap is the route.
class Keylens < Formula
  desc "TUI for Redis, Valkey and Recached that understands your keys"
  homepage "https://github.com/keylens/keylens"
  version "0.1.1"
  license "Apache-2.0"

  on_macos do
    on_arm do
      url "https://github.com/keylens/keylens/releases/download/v#{version}/keylens-aarch64-apple-darwin.tar.gz"
      sha256 "REPLACED_AT_RELEASE"
    end
    on_intel do
      url "https://github.com/keylens/keylens/releases/download/v#{version}/keylens-x86_64-apple-darwin.tar.gz"
      sha256 "REPLACED_AT_RELEASE"
    end
  end

  on_linux do
    on_arm do
      url "https://github.com/keylens/keylens/releases/download/v#{version}/keylens-aarch64-unknown-linux-gnu.tar.gz"
      sha256 "REPLACED_AT_RELEASE"
    end
    on_intel do
      url "https://github.com/keylens/keylens/releases/download/v#{version}/keylens-x86_64-unknown-linux-gnu.tar.gz"
      sha256 "REPLACED_AT_RELEASE"
    end
  end

  def install
    bin.install "keylens"
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/keylens --version")
  end
end
