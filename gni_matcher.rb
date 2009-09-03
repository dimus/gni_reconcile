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
  TMQUE = 'tm_que'
  TMERR = 'tm_err_que'
  BATCH_SIZE = 200
  
  def initialize
    config = YAML.load(open('config.yml'))
    @db = Mysql.init
    @db.options(Mysql::SET_CHARSET_NAME, 'utf8')
    @db.real_connect(config['host'], config['user'], config['password'], config['database'])
    @db.query("set character set utf8")
    @db_shared = Mysql.init
    @db_shared.options(Mysql::SET_CHARSET_NAME, 'utf8')
    @db_shared.real_connect(config['shared_host'], config['shared_user'], config['shared_password'], config['shared_database'])
    @db_shared.query("set character set utf8")
    @starling_host = config['starling_host']
  end

  def cursor
    return @db
  end
  
  def shared_cursor
    return @db_shared
  end
  
  def starling_host
    @starling_host
  end
end

class GniMatcher
  def initialize
    @db = Database.instance.cursor
    @tm = Taxamatch::Base.new
    @semantics = get_semantics
    @cache_strings_match = {}
    @cache_species_match = {}
    @cache_parsed = {}
  end

  def match_genera(genus1, genus1_id)
    result = {}
    genera = get_genera(genus1)
    genera.each do |genus2_id, genus2|
      match = @tm.match_genera(tm_prepare(genus1), tm_prepare(genus2))
      result[genus2_id.to_s] = {'normalized' => genus2, 'match' => match} if match['match']
    end
    
    @db.query "update genus_words set matched_data = '%s' where id = %s" % [Mysql.escape_string(result.to_json), genus1_id]
    result
  end

  def match_names(species1, genera_match, canonical1_id)
    genera_ids = genera_match.keys.join(",")
    return [] if genera_ids == ''
    canonical_ids = []
    get_species(genera_ids, species1, canonical1_id).each do |canonical2_id, genus2_id, species2| 
      species_match = match_species(species1, species2)
      if species_match['match']
        genus_match = genera_match[genus2_id.to_s]['match']
        binomial_match = @tm.match_matches(genus_match, species_match)
        canonical_ids << [canonical2_id, binomial_match['edit_distance']] if binomial_match['match']  
      end
    end
    canonical_ids
  end
  
  def match_name_strings(canonical1, canonicals)
    names1 = get_names(canonical1)
    matchers = compare_authors(names1, names1, 0)
    canonicals.each do |canonical, edit_distance|
      names2 = get_names(canonical)
      matchers += compare_authors(names1, names2, edit_distance)
    end
    matchers
  end

  protected
  
  def get_authors(id)
    if @cache_parsed[id]
      preparsed = @cache_parsed[id]
    else
      years = @db.query "select nw.word from name_words nw join name_word_semantics nws on nw.id = nws.name_word_id where nws.name_string_id = %s and semantic_meaning_id = %s" % [id, @semantics['year']]
      authors = @db.query "select nw.word from name_words nw join name_word_semantics nws on nw.id = nws.name_word_id where nws.name_string_id = %s and semantic_meaning_id = %s" % [id, @semantics['author_word']]
      all_years = []
      years.each do |year|
        all_years << year[0] if year[0].to_i > 1700 
      end
      all_authors = []
      authors.each do |auth|
        all_authors << auth
      end
      preparsed = {all_authors: all_authors, all_years: all_years} 
      @cache_parsed[id] = preparsed
    end
    preparsed
  end
  
  def compare_authors(names1, names2, edit_distance) 
    matchers = []
    names1.each do |id1, name1|
      auth1 = get_authors(id1)
      names2.each do |id2, name2|
        unless id1 == id2 || @cache_strings_match[ "%s|%s" % [id1,id2] ] || auth1[:all_authors].size == 0
          auth2 = get_authors(id2)
          match = auth2[:all_authors].size > 0 ? @tm.match_authors(auth1, auth2) : 0
          @cache_strings_match[ "%s|%s" % [id1, id2] ] = 1
          @cache_strings_match[ "%s|%s" % [id2, id1] ] = 1
          if match && match > 50
            matchers << [id1, id2, name1, name2, edit_distance, match]
            matchers << [id2, id1, name2, name1, edit_distance, match]
          end
        end
      end
    end
    matchers
  end
  
  def get_names(canonical_id)
    names = []
    @db.query("select ns.id, ns.name from name_strings ns join extended_canonical_forms ecf on ecf.id = ns.canonical_form_id where canonical_form_id = %s and ns.is_canonical_form = 0" % canonical_id).each do |id, name|
      names << [id, name]
    end
    names
  end

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
