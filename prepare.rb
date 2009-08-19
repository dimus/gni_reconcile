#!/usr/bin/env ruby1.9
# encoding: UTF-8

require 'gni_matcher'

db = Database.instance.cursor

['genus', 'species', 'infraspecies'].each do |t|

  puts "working on #{t}"

  db.query "drop table if exists #{t}_words"
  
  
  db.query "CREATE TABLE `#{t}_words` (
    `id` int(11) NOT NULL default '0',
    `normalized` varchar(100) default NULL,
    `first_letter` char(1) default NULL,
    `length` int(11) default NULL,
    `matched_data` text,
    PRIMARY KEY  (`id`),
    KEY `idx_#{t}_words_1` (`first_letter`)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8"
  
  db.query "insert into #{t}_words 
    select distinct nw.id, nw.word, nw.first_letter, nw.length, null 
      from name_word_semantics nws 
        join name_words nw on nw.id=nws.name_word_id 
        join semantic_meanings sm on sm.id = nws.semantic_meaning_id 
      where sm.name='#{t}' 
      order by normalized"
end


