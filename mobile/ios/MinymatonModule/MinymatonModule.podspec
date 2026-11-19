Pod::Spec.new do |s|
  s.name = "MinymatonModule"
  s.version = "0.1.0"
  s.summary = "React Native bridge module for the minymaton mobile core."
  s.homepage = "https://github.com/immutlex-dev/minymaton"
  s.license = { :type => "AGPL-3.0-only", :file => "../../../LICENSE" }
  s.author = { "immutlex-dev" => "295128224+immutlex-dev@users.noreply.github.com" }
  s.platform = :ios, "15.1"
  s.source = { :path => "." }
  s.source_files = "*.{h,mm}"
  s.frameworks = "Security"
  s.dependency "MinymatonCore"
  s.dependency "React-Core"
end
