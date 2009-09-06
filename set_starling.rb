#!/usr/bin/env ruby1.9
require 'starling'
require 'gni_matcher'

s = Starling.new(Database.instance.starling_host)
s.flush(Database::TMQUE)
s.flush(Database::TMERR)
db_shared = Database.instance.shared_cursor


#db_shared.query "drop table if exists taxamatchers"
#db_shared.query "drop table if exists taxamatch_statuses"
#
#db_shared.query "CREATE TABLE `taxamatchers` (
#  `id` int(11) NOT NULL auto_increment,
#  `name_string_id1` int(11) default NULL,
#  `name_string_id2` int(11) default NULL,
#  `edit_distance` int(11) default NULL,
#  `taxamatch_score` float default NULL,
#  `author_score` int(11) default NULL,
#  `matched` tinyint(1) default NULL,
#  `algorithmic` tinyint(1) default NULL,
#  `created_at` datetime default NULL,
#  `updated_at` datetime default NULL,
#  PRIMARY KEY  (`id`)
#) ENGINE=InnoDB DEFAULT CHARSET=utf8"
#
#db_shared.query "CREATE TABLE `taxamatch_statuses` (
#  finished_task int
#)"



rows_num = db_shared.query("SELECT count(*) FROM extended_canonical_forms where number_of_words = 2").fetch_row()[0]

s.flush(Database::TMQUE)
s.flush(Database::TMERR)

count = 0
while count < rows_num.to_i
  s.set(Database::TMQUE, count)
  count += Database::BATCH_SIZE
end

# ['p','c','a','s','m','t','e','h','l','d','b','n','g','o','r','i','k','f','v','z','x','u','w','j','y','q'].reverse.each do |letter|
#   if ['p','c','a','s','m','t','e','h','l','d'].include? letter
#     ('a'..'z').to_a.reverse.each do |letter2|
#       s.set(Database::TMQUE, letter + letter2)
#     end
#   else
#     s.set(Database::TMQUE, letter)
#   end
# end
# 
