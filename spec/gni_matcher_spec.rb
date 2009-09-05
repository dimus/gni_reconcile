# encoding: UTF-8
require File.dirname(__FILE__) + '/spec_helper.rb'

NAMES = ['ACTINOMYCES RADINGAE']

describe 'gni_mathcher' do
  it 'should find matches for names' do 
    NAMES.each do |name|
      word1, word2 = name.split(" ")
      res = db.query "SELECT id FROM canonical_forms WHERE name = %s" % name
      id = res.fetch_row[0]
      puts id, word1, word2
    end
  end
end
