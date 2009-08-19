#!/usr/bin/env ruby1.9
TMQUE = 'tm_que'
require 'starling'

s = Starling.new(ARGV[0])
s.flush(TMQUE)

['p','c','a','s','m','t','e','h','l','d','b','n','g','o','r','i','k','f','v','z','x','u','w','j','y','q'].reverse.each do |letter|
  if ['p','c','a','s','m','t','e','h','l','d'].include? letter
    ('a'..'z').to_a.reverse.each do |letter2|
      s.set(TMQUE, letter + letter2)
    end
  else
    s.set(TMQUE, letter)
  end
end

