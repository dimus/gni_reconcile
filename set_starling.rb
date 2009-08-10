#!/usr/bin/env ruby1.9

require 'starling'

s = Starling.new('10.19.19.58:22122')

('a'..'z').to_a.reverse.each do |letter|
  s.set('r_cue', letter)
end
