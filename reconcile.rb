#!/usr/bin/env ruby1.9
# encoding: UTF-8
require 'gni_matcher'
require 'optparse'

OPTIONS = {
  :que_host => nil,
  :letter => nil
}

ARGV.options do |opts|
  script_name = File.basename($0)
  opts.banner = "Usage: ruby #{script_name} [options]"

  opts.separator ""

  opts.on("-h", "--host=host", String,
          "Host of a starling daemon",
          "Default: nil") { |opt| OPTIONS[:que_host] = opt }
  
  opts.separator ""

  opts.on("-l", "--letter=letter", String,
          "Letter to process",
          "Default: nil") { |opt| OPTIONS[:letter] = opt }

  opts.separator ""

  opts.on("-h", "--help",
          "Show this help mesage.") { puts opts; exit }

  opts.parse!
end



def reconcile(letter, db)
  data_file = "results/" + letter + ".txt"
  f = open(data_file, 'w')
  gm = GniMatcher.new

  res = db.query("SELECT id, word1, word2 FROM extended_canonical_forms WHERE number_of_words=2 and word1 like '%s%%' order by word1, word2 limit 100" % letter)

  puts "%s letter rows to process: %s" % [letter,res.num_rows]

  count = 0
  res.each do |canonical_id, genus, species|
    count += 1
    print "%s: %s " % [letter,count] if count % 100 == 0
    next if genus == '' || genus == nil
    f.write "Canonical: %s %s\n" % [genus, species]
    genus_id, genus_match = db.query("select id, matched_data from genus_words where normalized = '%s'" % genus).fetch_row
    if genus_id
      genus_match = genus_match ? JSON.load(genus_match) : gm.match_genera(genus, genus_id)
      canonical_ids = gm.match_names(species, genus_match, canonical_id)
      name_strings1, name_strings2 = gm.get_name_strings(canonical_id, canonical_ids)
      matchers = gm.match_name_strings(name_strings1, name_strings2)
      matchers.each do |name1, name2, edit_distance|
        f.write "    %s\n    %s\n    ed.dist: %s\n\n" % [name1, name2,edit_distance]
      end
    else
      f.write "Did not find %s in genus_word table\n\n" % genus
    end
  end
  f.close
end

if $0 == __FILE__

  letter = OPTIONS[:letter] || 'q'
  host = OPTIONS[:que_host]
  db = Database.instance.cursor
  if host
    require 'starling'
    s = Starling.new(host)
    while 1
      letter = s.get('r_que')
      reconcile(letter, db)
    end
  else
    reconcile(letter, db)
  end

end
