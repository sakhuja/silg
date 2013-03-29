require 'yaml'
require 'logger'
require 'fileutils'
require 'csv'

class Orgs_processor

 def initialize(inputlog, filteredlog, orgs_csv)
    @@file_path = File.expand_path(File.dirname(__FILE__))
    @@orgs_csv = "../../../data/input/orgs/#{orgs_csv}"
    @@logs_dest= "../../../data/output/load/"
    @@filteredlog = "../../../data/output/load/#{filteredlog}"
    @@logs_source = "../../../data/input/#{inputlog}"
    puts @@orgs_csv
    puts @@logs_dest
    puts @@filteredlog
    puts @@logs_source

 end

 def get_orgs(orgs_file)
    orgs = @@file_path + "/" + orgs_file
    lst_orgs = []
    if orgs.class.to_s == "String"
      if File.exists?(orgs_file)
        puts "reading orgs from file : #{orgs_file}"
        lst_orgs = CSV.read(orgs_file)
      else
        raise "File not found."
      end
    end
    return lst_orgs
 end

 def filter
    # puts "lst_orgs :" + lst_orgs.class.to_s
    lst_orgs=get_orgs(@@orgs_csv)
    %x["rm -rf #{@@logs_dest}/#{@@filteredlog}"]
    lst_orgs.flatten.each { | org |
      org = org.gsub(/\"/, "") 
      str_filter = " | grep '#{org}'"
      org_log = @@logs_dest + "/" + org + "_I_lines.log"
      #puts "#{org_log}"
      cmd = "cat #{@@logs_source} #{str_filter} > #{org_log}"
      #puts cmd
      %x[ #{cmd} ]
      cmd_filteredlog  = "cat #{org_log} >> #{@@logs_dest}/#{@@filteredlog}"
      puts cmd_filteredlog
      #exit 1
      %x[#{cmd_filteredlog} ]
    }   
 end 

puts "Starting..."

# quit unless our script gets two command line arguments
unless ARGV.length == 3
  puts "Dude, wrong args count."
  puts "Example Usage: jruby orgs_filter.rb Add.all_Ilines.log filtered_Ilines.log cm_15_20_sp_orgsonly.csv \n"
  exit
end

inputlog = ARGV[0]
filteredlog = ARGV[1]
orgs_csv = ARGV[2]

#inputlog = "Add.all_Ilines.log"
#filteredlog = "filtered_Ilines.log"
#orgs_csv = "cm_15_20_sp_orgsonly.csv"

op = Orgs_processor.new(inputlog, filteredlog, orgs_csv)
op.filter
puts "Done."

end

