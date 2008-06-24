require 'dbf/dbf'
require 'iconv'
require 'sqlite3'
$KCODE = 'u'

class Kladr

  def prepare_database
	  @db = SQLite3::Database.new("kladr.sqlite3")
	end

  
  def file_unpack(file)
    return if File.exists?(file)
    return unless File.exists?(file+".gz")
    `gzip -cd #{file}.gz > #{file}`
  end
  
  def recode(string)
    string && Iconv.iconv("UTF-8", "CP866", string).first
  end

	def extract_kladr_code(record, have_street = false)
    region_code = record.attributes["code"][0,2].to_i
		district_code = record.attributes["code"][2,3].to_i
		city_code = record.attributes["code"][5,3].to_i
		town_code = record.attributes["code"][8,3].to_i
		if have_street
      street_code = record.attributes["code"][11, 4].to_i
      actuality_code = record.attributes["code"][15,2].to_i
    else
      street_code = nil
      actuality_code = record.attributes["code"][11,2].to_i
    end
    name = recode(record.attributes["name"])
		abbrev = recode(record.attributes["socr"])
		[region_code, district_code, city_code, town_code, name, abbrev, street_code, actuality_code]
	end

	def areas_import(file = File.dirname(__FILE__)+"/../BASE/KLADR.DBF")
    start_time = Time.now
    file_unpack(file)
    table = Kladr::DBF::Table.new(file, :in_memory => false)
    table.columns.each {|c| c.name.replace(c.name.downcase) }
    @db.execute("DROP TABLE IF EXISTS areas")
    @db.execute("CREATE TABLE areas (
		  id integer primary key autoincrement, 
		  region_code integer,
			district_code integer,
			city_code integer,
			town_code integer,
			name varchar(60),
			abbrev varchar(10),
			postal_code integer
			)")
		
    
    puts "Table areas created, importing #{table.record_count} records"
    count = 0
		insert = @db.prepare("INSERT INTO areas (region_code, district_code, city_code, town_code, name, abbrev, postal_code) VALUES (?,?,?,?,?,?,?)")

    0.upto(table.record_count-1) do |i|
      record = table.record(i) 
      next unless record
      kladr_code = extract_kladr_code(record, false)

			actuality_code = kladr_code.pop
      next unless actuality_code == 0
      street_code = kladr_code.pop
      kladr_code << record.attributes["index"].to_i
			count += 1
			puts count if count % 1000 == 0
			insert.execute!(*kladr_code)
		end
		
    import_time = Time.now
    @db.execute("CREATE UNIQUE INDEX areas_index ON areas (region_code, district_code, city_code, town_code)")
    puts "It took #{import_time - start_time} seconds to import #{count} records. #{Time.now - import_time} to build index."
	end
  
  def street_import(file = File.dirname(__FILE__)+"/../BASE/STREET.DBF")
    start_time = Time.now
    file_unpack(file)
    table = Kladr::DBF::Table.new(file, :in_memory => false)
    table.columns.each {|c| c.name.replace(c.name.downcase) }
    @db.execute("DROP TABLE IF EXISTS streets")
		@db.execute("CREATE TABLE streets (
		  id integer primary key autoincrement, 
		  region_code integer,
			district_code integer,
			city_code integer,
			town_code integer,
			street_code integer,
			name varchar(60),
			abbrev varchar(10),
			area_id integer
			)")
    
    puts "Table streets created, importing #{table.record_count} records"
    count = 0
		insert = @db.prepare("INSERT INTO streets (region_code, district_code, city_code, town_code, name, abbrev, street_code, area_id) VALUES (?,?,?,?,?,?,?,?)")
		find = @db.prepare("SELECT id FROM areas WHERE region_code = ? AND district_code = ? AND city_code = ? AND town_code = ? LIMIT 1")

    0.upto(table.record_count-1) do |i|
      record = table.record(i) 
      next unless record
			
      kladr_code = extract_kladr_code(record, true)
			actuality_code = kladr_code.pop
      next unless actuality_code == 0
			
			find.execute!(kladr_code[0,4]) do |row|
			  kladr_code << row[0].to_i
        count += 1
  			puts count if count % 1000 == 0
  			insert.execute!(*kladr_code)
		  end


      #puts ("%4d %s %s" % [street_code, abbrev, name]) if region_code ==77
      #count += 1 if region_code == 77
      #if count == 1
      #  puts "Starting Moscow on #{i} record"
      #end
    end
    import_time = Time.now
    @db.execute("CREATE UNIQUE INDEX streets_index ON streets (region_code, district_code, city_code, town_code, street_code)")
    @db.execute("CREATE INDEX streets_area_index ON streets (area_id)")
    puts "It took #{import_time - start_time} seconds to import #{count} records. #{Time.now - import_time} to build index."
  end
  
  def houses_import(file = File.dirname(__FILE__)+"/../BASE/DOMA.DBF")
    start_time = Time.now
    file_unpack(file)
    table = Kladr::DBF::Table.new(file, :in_memory => false)
    table.columns.each {|c| c.name.replace(c.name.downcase) }

    @db.execute("DROP TABLE IF EXISTS houses")
		@db.execute("CREATE TABLE houses (
		  id integer primary key autoincrement, 
		  region_code integer,
			district_code integer,
			city_code integer,
			town_code integer,
			street_code integer,
			house_code integer,
			number varchar(10),
			building integer,
			postal_code integer,
			street_id integer
			)")
    
    puts "Table houses created, importing #{table.record_count} records"
    count = 0
    
    
		insert = @db.prepare("INSERT INTO houses (region_code, district_code, city_code, town_code, street_code, street_id, building, postal_code, number) VALUES (?,?,?,?,?,?,?,?,?)")
		find = @db.prepare("SELECT id FROM streets WHERE region_code = ? AND district_code = ? AND city_code = ? AND town_code = ? AND street_code = ? LIMIT 1")

    0.upto(table.record_count-1) do |i|
      record = table.record(i) 
      next unless record

      kladr_code = extract_kladr_code(record, true)
			actuality_code = kladr_code.pop
      next unless actuality_code == 0
			
			numbers_packed = kladr_code.delete_at(4)
			abbrev = kladr_code.delete_at(4)
			# kladr_code is now region_code, district_code, city_code, town_code, street_code
			
			find.execute!(kladr_code[0,5]) do |row|
			  kladr_code << row[0].to_i
			  kladr_code << record.attributes["korp"].to_i
			  kladr_code << record.attributes["index"].to_i
			  numbers = extract_numbers(numbers_packed)
			  puts "#{numbers_packed} -> #{numbers.inspect}" if kladr_code[0,5] == [77,0,0,0,794]
			  numbers.each do |number|
			    insert.execute!(*(kladr_code + [number]))
		    end
        count += 1
  			puts count if count % 1000 == 0
		  end
    end
    import_time = Time.now
    @db.execute("CREATE INDEX houses_index ON houses (street_id)")
    puts "It took #{import_time - start_time} seconds to import #{count} records. #{Time.now - import_time} to build index."
  end
  
  def extract_numbers(numbers)
    return [] unless numbers
    numbers.split(",").map do |part|
      if part.index("-")
        start_number, end_number = /(\d+)-(\d+)/.match(part).captures.map {|c| c.to_i}
        step = part.index("(") ? 2 : 1
        res = []
        (start_number..end_number).step(step) {|i| res << i.to_s}
        res
      else
        part
      end
    end.flatten
  end
  
  
  def import
	  prepare_database
		areas_import
    street_import
    houses_import
  end  
  
end
