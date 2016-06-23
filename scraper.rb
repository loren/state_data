require 'scraperwiki'
require 'httparty'
require 'date'

ENDPOINT = 'http://bids.state.gov/geoserver/opengeo/ows?service=WFS&version=1.0.0&request=GetFeature&srsName=EPSG:4326&typeName=opengeo%3ADATATABLE&outputformat=json&FILTER=%3CFilter%3E%0A%3CPropertyIsEqualTo%3E%0A%09%09%09%3CPropertyName%3ECleared%3C%2FPropertyName%3E%0A%09%09%09%3CLiteral%3E1%3C%2FLiteral%3E%0A%09%09%3C%2FPropertyIsEqualTo%3E%0A%3C%2FFilter%3E'

def clean_table
  ScraperWiki.sqliteexecute('DELETE FROM data')
rescue SqliteMagic::NoSuchTable
  puts "Data table does not exist yet"
end

def squish(str)
  str.gsub(/\A[[:space:]]+/, '').gsub(/[[:space:]]+\z/, '').gsub(/[[:space:]]+/, ' ')
end

def fetch_results
  response = HTTParty.get(ENDPOINT)
  results = JSON.parse(response.body, symbolize_names: true)
  results[:features].select { |article_hash| valid_entry?(article_hash[:properties]) }.
    map { |article_hash| process_entry_info(article_hash) }.
    each { |article_hash| ScraperWiki.save_sqlite(%i(id), article_hash) }
end

def process_entry_info(entry_hash)
  entry = entry_hash[:properties]
  entry[:id] = entry_hash[:id]
  %i(Project_Announced Tender_Date).each do |field|
    entry[field] &&= Date.parse(entry[field]).iso8601
  end
  %i(Post_Comments Project_Description Project_Title Keyword Project_POCs).each do |field|
    entry[field] &&= squish(entry[field])
  end
  entry
end

def valid_entry?(entry)
  entry[:Tender_Date].nil? || Date.strptime(entry[:Tender_Date]) >= Date.today
end

clean_table
fetch_results
