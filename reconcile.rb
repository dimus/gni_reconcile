#!/usr/bin/env ruby1.9
# encoding: UTF-8
require 'gni_matcher'
require 'optparse'

OPTIONS = {
  :cluster => false,
  :limit => nil
}

ARGV.options do |opts|
  script_name = File.basename($0)
  opts.banner = "Usage: ruby #{script_name} [options]"

  opts.separator ""

  opts.on("-c", "--cluster", String,
          "run in a cluster mode",
          "Default: false") { |opt| OPTIONS[:cluster] = true }
  
  opts.separator ""

  opts.on("-l", "--limit=limit", String,
          "Letter to process",
          "Default: nil") { |opt| OPTIONS[:limit] = opt }

  opts.separator ""

  opts.on("-h", "--help",
          "Show this help mesage.") { puts opts; exit }

  opts.parse!
end



def reconcile(limit, db, db_shared)
  data_file = "results/#{limit.to_s}_of#{Database::BATCH_SIZE}.txt"
  f = open(data_file, 'w')
  gm = GniMatcher.new

  res = db.query("SELECT id, word1, word2 FROM extended_canonical_forms WHERE number_of_words=2 order by word1, word2 limit %s, %s" % [limit, Database::BATCH_SIZE])

  puts "%s limit rows to process: %s" % [limit,res.num_rows]

  count = 0
  res.each do |canonical_id, genus, species|
    count += 1
    print "%s: %s " % [limit,count] if count % 100 == 0
    next if genus == '' || genus == nil
    f.write "#Canonical: %s %s\n" % [genus, species]
    genus_id, genus_match = db.query("select id, matched_data from genus_words where normalized = '%s'" % genus).fetch_row
    if genus_id
      genus_match = genus_match ? JSON.load(genus_match) : gm.match_genera(genus, genus_id)
      canonical_ids = gm.match_names(species, genus_match, canonical_id)
      matchers = gm.match_name_strings(canonical_id, canonical_ids)
      matchers.each do |id1, id2, name1, name2, edit_distance, auth_score|
        distance_score = (1 - edit_distance.to_f/((name1.size + name2.size)/2.0)) * 100
        f.write "%s\t%s\t%s\t%s\t%s\n\n" % [edit_distance, auth_score, distance_score, name1, name2]
        query = "insert IGNORE into taxamatchers (name_string_id1, name_string_id2, edit_distance, taxamatch_score, author_score, matched, algorithmic, created_at, updated_at) values (%s, %s, %s, '%s', %s, 1, 1, now(), now())" % [id1, id2, edit_distance, distance_score, auth_score]
        db_shared.query(query)
      end
    else
      f.write "Did not find %s in genus_word table\n\n" % genus
    end
  end
  f.close
  db_shared.query("insert into taxamatch_statuses (finished_task) values ('%s')" % limit)
  puts
end


if $0 == __FILE__
  
  limit = OPTIONS[ :limit ] || 0
  cluster = OPTIONS[ :cluster ] || false
  db_inst = Database.instance
  db = db_inst.cursor
  db_shared = db_inst.shared_cursor

  if cluster
    require 'starling'
    puts 'starting reconciliation in cluster mode'
    s = Starling.new( db_inst.starling_host )
    while 1
      limit = s.get( Database::TMQUE )
      limit_is_processed = db_shared.query("select 1 from taxamatch_statuses where finished_task = %s" % limit).fetch_row
      puts "skipping %s" %limit if limit_is_processed
      next if limit_is_processed
      begin
        reconcile( limit, db, db_shared )
      rescue Exception => e
        puts "#{ e } (#{ e.class })!"
        puts "#{ limit } will be rescheduled"
        s.set( Database::TMERR, limit )
      end
    end
  else
    reconcile( limit, db, db_shared )
  end

end
