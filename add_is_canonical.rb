#!/usr/bin/env ruby1.9
# encoding: UTF-8

require 'gni_matcher'
require 'biodiversity'

db = Database.instance.cursor
parser = ScientificNameParser.new

min = ARGV[0]
max = ARGV[1]
puts "checking canonical forms between %s and %s" % [min, max]
names_data = db.query("select id, name from name_strings where is_canonical_form is null and id between %s and %s order by id" % [min, max])

count = 0
db.query('start transaction')
names_data.each do |id, name|
  count += 1
  if count % 10000 == 0
    puts count
    parser = ScientificNameParser.new
    db.query('commit')
    db.query('start transaction')
  end
  name = name.force_encoding('utf-8')
  parsed = parser.parse(name) rescue nil
  
  is_canonical =  (parsed && parsed[:scientificName][:canonical] == name) ? 1 : 0
  db.query("UPDATE name_strings set is_canonical_form = %s where id = %s" % [is_canonical, id]) 
end
db.query('commit')
