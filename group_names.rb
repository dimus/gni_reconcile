#!/usr/bin/env ruby1.9
# encoding: UTF-8
require 'gni_matcher'


class TaxaGroup
  def initialize
    @db = Database.instance.cursor
    @lexical_groups_table = "lexical_groups"
    @lexical_group_name_strings_table = "lexical_group_name_strings"
  end


  def set_tables 
    puts "creating tables"
    @db.query "drop table if exists %s" % @lexical_groups_table
    @db.query "CREATE TABLE `%s` (
      `id` int(11) NOT NULL auto_increment,
      `supercedure_id` int(11) default NULL,
      `created_at` datetime default NULL,
      `updated_at` datetime default NULL,
      PRIMARY KEY  (`id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8" % @lexical_groups_table

    @db.query "drop table if exists %s" % @lexical_group_name_strings_table
    @db.query "CREATE TABLE `%s` (
      `id` int(11) NOT NULL auto_increment,
      `name_string_id` int(11) default NULL,
      `lexical_group_id` int(11) default NULL,
      `created_at` datetime default NULL,
      `updated_at` datetime default NULL,
      PRIMARY KEY  (`id`),
      UNIQUE KEY `idx_lexical_group_name_strings_1` (`name_string_id`,`lexical_group_id`),
      KEY `idx_lexical_group_name_strings_2` (`lexical_group_id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8" % @lexical_group_name_strings_table
  end  
  
  def create_canonical_groups
    puts "creating groups with canonical forms"
    name_ids = @db.query "select id, canonical_form_id from name_strings where is_canonical_form = 0 and canonical_form_id is not null and canonical_form_id != 0 order by name"
    count = 0
    @db.query "start transaction"
    name_ids.each do |id, canonical_form_id|
      count += 1
      lg_id = create_group
      res = @db.query "select id from name_strings where canonical_form_id = %s and is_canonical_form = 1" % canonical_form_id
      canonical = res.fetch_row
      if canonical
        @db.query "insert into %s (name_string_id, lexical_group_id, created_at, updated_at) values (%s, %s, now(), now()),(%s, %s, now(), now())" % [@lexical_group_name_strings_table, canonical[0], lg_id, id, lg_id]
      end
      if count % 1000 == 0
        puts count
        @db.query "commit"
        @db.query "start transaction"
      end
    end
    @db.query "commit"
  end

  def create_taxamatch_groups
    puts "creating taxamatch groups"
    tm_res = @db.query "select name_string_id1, name_string_id2 from taxamatchers" % @lexical_group_name_strings_table
    @db.query "start transaction"
    count = 0
    tm_res.each do |id1, id2|
      count += 1
      if count % 1000 == 0
        puts count
        @db.query "commit"
        @db.query "start transaction"
      end
      gid1 = get_group_id(id1)
      gid2 = get_group_id(id2)
      if gid1 && gid2
        next if gid1 == gid2
        @db.query "update IGNORE %s set lexical_group_id = %s where lexical_group_id = %s" % [@lexical_group_name_strings_table, gid1, gid2]
        @db.query "delete from %s where lexical_group_id = %s" % [@lexical_group_name_strings_table, gid2]
      elsif gid1
        @db.query "insert into %s (name_string_id, lexical_group_id, created_at, updated_at) values (%s, %s, now(), now())" % [@lexical_group_name_strings_table, id2, gid1] 
      elsif gid2
        @db.query "insert into %s (name_string_id, lexical_group_id, created_at, updated_at) values (%s, %s, now(), now())" % [@lexical_group_name_strings_table, id1, gid2] 
      else
        gid3 = create_group
        @db.query "insert into %s (name_string_id, lexical_group_id, created_at, updated_at) values (%s, %s, now(), now()), (%s, %s, now(), now())" % [@lexical_group_name_strings_table, id1, gid3, id2, gid3]
      end
      
    end
    @db.query "commit"
  end

  def groups_cleanup
    @db.query "delete l from lexical_groups_tmp l left join lexical_group_name_strings_tmp ln on l.id = ln.lexical_group_id where ln.id is null"
  end

  def export
    f = open('results/lexical_groups.txt', 'w')
    res = @db.query "select l.lexical_group_id, ns.name from name_strings ns join %s l on l.name_string_id = ns.id where l.lexical_group_id is not null order by l.lexical_group_id" % @lexical_group_name_strings_table
    res.each do |group_id, name|
      f.write "%s\t%s\n" % [group_id, name]
    end
    f.close
  end
 
 protected

  def create_group
      @db.query "insert into %s (created_at, updated_at) values (now(), now())" % @lexical_groups_table
      @db.insert_id 
  end
  
  def get_group_id(name_string_id)
    gid = @db.query "select lexical_group_id from %s where name_string_id = %s" % [@lexical_group_name_strings_table, name_string_id]
    gid = gid.fetch_row
    gid = gid[0] if gid
    gid
  end
end

if $0 == __FILE__
  t = TaxaGroup.new 
  #t.set_tables
  #t.create_canonical_groups
  #t.create_taxamatch_groups
  #t.groups_cleanup
  t.export
end
