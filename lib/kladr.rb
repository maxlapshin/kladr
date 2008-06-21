require 'dbf/dbf'
require 'iconv'
require 'sqlite3'
$KCODE = 'u'

class Kladr

  def prepare_database
	  @db = SQLite3::Database.new("kladr.sqlite3")
		kladr_code = "
		  region_code integer,
			district_code integer,
			city_code integer,
			town_code integer,
			"
		@db.execute("CREATE TABLE IF NOT EXISTS streets (
		  id integer primary key autoincrement, 
			#{kladr_code}
			street_code integer,
			name varchar(60),
			abbrev varchar(10)
			)")
		@db
	end

  
  def file_unpack(file)
    return if File.exists?(file)
    return unless File.exists?(file+".gz")
    `gzip -cd #{file}.gz > #{file}`
  end
  
  def recode(string)
    Iconv.iconv("UTF-8", "CP866", string).first
  end

	def extract_kladr_code(record)
    region_code = record.attributes["code"][0,2].to_i
		district_code = record.attributes["code"][2,3].to_i
		city_code = record.attributes["code"][5,3].to_i
		town_code = record.attributes["code"][8,3].to_i
    street_code = record.attributes["code"][11, 4].to_i
    actuality_code = record.attributes["code"][15,2].to_i
    name = recode(record.attributes["name"])
		abbrev = recode(record.attributes["socr"])
		[region_code, district_code, city_code, town_code, street_code, name, abbrev, actuality_code]
	end

	def kladr_import(file = File.dirname(__FILE__)+"/../BASE/KLADR.DBF")
    start_time = Time.now
    file_unpack(file)
    table = Kladr::DBF::Table.new(file, :in_memory => false)
    table.columns.each {|c| c.name.replace(c.name.downcase) }
    
    puts "Table kladr created, importing #{table.record_count} records"
    count = 0
		insert = @db.prepare("INSERT INTO streets (region_code, district_code, city_code, town_code, street_code, name, abbrev) VALUES (?,?,?,?,?,?,?)")

    0.upto(table.record_count-1) do |i|
      record = table.record(i) 
      next unless record
      kladr_code = extract_kladr_code(record)
			puts kladr_code.inspect
			count += 1
			#exit if count == 400
			next
			
			actiality_code = kladr_code.pop
      next unless actuality_code == 0
			
      kladr_code << (name = recode(record.attributes["name"]))
		end
	end
  
  def street_import(file = File.dirname(__FILE__)+"/../BASE/STREET.DBF")
    start_time = Time.now
    file_unpack(file)
    table = Kladr::DBF::Table.new(file, :in_memory => false)
    table.columns.each {|c| c.name.replace(c.name.downcase) }
    
    puts "Table created, importing #{table.record_count} records"
    count = 0
		insert = @db.prepare("INSERT INTO streets (region_code, district_code, city_code, town_code, street_code, name, abbrev) VALUES (?,?,?,?,?,?,?)")

    0.upto(table.record_count-1) do |i|
      record = table.record(i) 
      next unless record
			
      kladr_code = extract_kladr_code(record)
			actiality_code = kladr_code.pop
      next unless actuality_code == 0
			

			insert.execute!(*kladr_code)

      #puts ("%4d %s %s" % [street_code, abbrev, name]) if region_code ==77
      #count += 1 if region_code == 77
      #if count == 1
      #  puts "Starting Moscow on #{i} record"
      #end
    end
    import_time = Time.now
    
    puts "It took #{import_time - start_time} seconds to import #{count} records. #{Time.now - import_time} to build index."
  end
  
  def self.exec_houses_schema
    ActiveRecord::Migration.create_table "houses" do |t|
      t.column "number", :string, :limit => 10
      t.column "street_code", :integer
      t.column "abbrev", :string, :limit => 10
      t.column "building", :integer
      t.column "index", :integer
      t.column "house_code", :integer
      t.column "street_id", :integer
    end
    ActiveRecord::Migration.add_index :houses, [:street_code, :house_code]
  end
  
  def houses_import(file = File.dirname(__FILE__)+"/../BASE/DOMA.DBF")
    start_time = Time.now
    file_unpack(file)
    table = Kladr::DBF::Table.new(file, :in_memory => false)
    table.columns.each {|c| c.name.replace(c.name.downcase) }
    
    exec_houses_schema rescue false
    
    puts "Table created, importing #{table.record_count} records"
    count = 0
    table_columns = House.columns.map(&:name)
    
    0.upto(table.record_count-1) do |i|
      record = table.record(i) 
      next unless record
      
      city_code = record.attributes["code"][0,2].to_i
      street_code = record.attributes["code"][11, 4].to_i
      house_code = record.attributes["code"][15, 4].to_i
      next unless city_code == 77
      next if street_code == 0
      attributes = {
        :street_code => street_code, :house_code => house_code, :abbrev => recode(record.attributes["socr"]).chars.downcase.to_s,
        :building => recode(record.attributes["korp"]), :index => record.attributes["index"].to_i
      }.reject {|field, value| !table_columns.include?(field.to_s)}
      attributes[:numbers] = recode(record.attributes["name"])
      street = Street.find_by_street_code(street_code)
      attributes[:street_id] = street.id if street
      houses = create_houses(attributes)
      count += houses.length
      if count == 1
        puts "Starting Moscow on #{i} record"
      end
    end
    puts "It took #{Time.now - start_time} seconds to import #{count} records."  
  end
  
  def self.extract_numbers(numbers)
    return [] unless numbers
    numbers.split(",").map do |part|
      if part.index("-")
        start_number, end_number = /(\d+)-(\d+)/.match(part).captures.map(&:to_i)
        step = part.index("(") ? 2 : 1
        res = []
        (start_number..end_number).step(step) {|i| res << i.to_s}
        res
      else
        part
      end
    end.flatten
  end
  
  def self.create_houses(attributes)
    numbers = extract_numbers(attributes.delete(:numbers))
    numbers.each do |number|
      house = House.create(attributes.merge(:number => number))
      puts ("%30s %4s" % [house.street && house.street.name || "-", house.number]) 
    end
  end
  
  def import
	  prepare_database
		kladr_import
    street_import
    #houses_import
  end  
  
end
