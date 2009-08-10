#!/usr/bin/env ruby1.9

require 'starling'

s = Starling.new(ARGV[0])

('a'..'z').to_a.reverse.each do |letter|
  s.set('r_que', letter)
end
