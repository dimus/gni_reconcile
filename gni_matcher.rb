# encoding: UTF-8

if RUBY_VERSION.gsub('.','').to_i < 191
  puts 'works only with ruby versions 1.9.1 and higher'
  exit
end

require 'singleton'
require 'mysql'
require 'taxamatch_rb'
require 'json'

class Database
  include Singleton
  
  def initialize
    config = YAML.load(open('config.yml'))
    @db = Mysql.init
    @db.options(Mysql::SET_CHARSET_NAME, 'utf8')
    @db.real_connect(config['host'], config['user'], config['password'], config['database'])
    @db.query("set names utf8")
  end

  def cursor
    return @db
  end
end

class GniMatcher
  def initialize
    @db = Database.instance.cursor
    @tm = Taxamatch::Base.new
    @semantics = get_semantics
    @cache_strings_match = {}
    @cache_species_match = {}
  end

  def match_genera(genus1, genus1_id)
    result = {}
    genera = get_genera(genus1)
    
    genera.each do |genus2_id, genus2|
      match = @tm.match_genera(tm_prepare(genus1), tm_prepare(genus2))
      result[genus2_id.to_s] = {normalized: genus2, match: match} if match[:match]
    end
    
    @db.query "update genus_words set matched_data = '%s' where id = %s" % [Mysql.escape_string(result.to_json), genus1_id]
    result
  end

  def match_names(species1, genera_match, canonical1_id)
    genera_ids = genera_match.keys.join(",")
    return if genera_ids == ''
    canonical_ids = []
    get_species(genera_ids, species1, canonical1_id).each do |canonical2_id, genus2_id, species2| 
      species_match = match_species(species1, species2)
      if species_match['match']
        genus_match = genera_match[genus2_id.to_s]['match']
        binomial_match = @tm.match_matches(genus_match, species_match)
        canonical_ids << canonical2_id if binomial_match['match']
      end
    end
    canonical_ids
  end

  def get_name_strings(canonical1_id, canonical_ids)
    canonical_ids << canonical1_id rescue canonical_ids = [canonical1_id]
    matchers = @db.query "select id, name from name_strings where canonical_form_id = %s" % canonical1_id
    to_match = @db.query "select id, name from name_strings where canonical_form_id in (%s)" % canonical_ids.join(",")
    [matchers, to_match]
  end

  def match_name_strings(name1_strings, name2_strings)
    matchers = []
    names1 = []
    names2 = []
    name1_strings.each do |id, name|
      names1 << [id, name]
    end

    name2_strings.each do |id, name|
      names2 << [id, name]
    end

    names1.each do |id1, name1|
      name1 = name1.force_encoding('utf-8')
      names2.each do |id2, name2|
        unless id1 == id2 || @cache_strings_match[ "%s|%s" % [id1,id2] ] 
          name2 = name2.force_encoding('utf-8')
          match = @tm.taxamatch(name1, name2)
          @cache_strings_match[ "%s|%s" % [id1, id2] ] = match
          @cache_strings_match[ "%s|%s" % [id2, id1] ] = match
          if match 
            matchers << [name1, name2]
            matchers << [name2, name1]
          end
        end
      end
    end
    matchers 
  end

  protected
  def match_species(species1, species2)
    species_key = [species1, species2].sort.join("|")
    return @cache_species_match[ species_key ] if @cache_species_match[ species_key ]
    match = @tm.match_species(tm_prepare(species1), tm_prepare(species2))
    @cache_species_match[ species_key ] = match
    match
  end

  def get_species(genera_ids, species1, canonical1_id)
    length = length_min_max species1
    query  = "select id, word1_id, word2 from extended_canonical_forms where word1_id in (%s) and number_of_words = 2 and id != %s and word2 like '%s%%' and word2_length between %s and %s" % [genera_ids, canonical1_id, species1[0], length[:min], length[:max]]
    @db.query query
  end

  def tm_prepare(word)
    {normalized: word, phonetized: Taxamatch::Phonetizer.near_match(word)} 
  end

  def get_genera(genus)
    length = length_min_max genus
    @db.query "select id, normalized from genus_words where first_letter = '%s' and length between %s and %s" % [genus[0], length[:min], length[:max]]
  end

  def length_min_max(word)
    length = word.size
    delta = (length/5.0).round
    {min: length - delta, max: length + delta} 
  end

  def get_semantics
    semantics = {}
    @db.query("select id, name from semantic_meanings").each {|id, name| semantics[name] = id}
    semantics   
  end
end
