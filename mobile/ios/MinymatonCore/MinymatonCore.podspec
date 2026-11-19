Pod::Spec.new do |s|
  s.name = "MinymatonCore"
  s.version = "0.1.0"
  s.summary = "Rust minymaton-mobile-core static library + Swift bridge."
  s.homepage = "https://github.com/immutlex-dev/minymaton"
  s.license = { :type => "AGPL-3.0-only", :file => "../../../LICENSE" }
  s.author = { "immutlex-dev" => "295128224+immutlex-dev@users.noreply.github.com" }
  s.platform = :ios, "15.1"
  s.source = { :path => "." }
  s.source_files = "Sources/**/*.{swift,h}"
  s.public_header_files = "Sources/MinymatonCoreFFI.h"
  s.vendored_libraries = "Libraries/libminymaton_mobile_core.a"
  s.pod_target_xcconfig = { "OTHER_LDFLAGS" => "-lc++ -lresolv" }
  s.swift_version = "5.0"
end
