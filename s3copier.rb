#!/usr/bin/env ruby

require 'rubygems'
require 'aws/s3'

class S3Copier
  
  def initialize(args = { })
    @path        = args[:path]
    @src         = args[:src]
    @dest        = args[:dest]
    @dry         = args[:dry]
    @max_retries = (args[:max_retries] || 3)
    
    @dir = File.expand_path(File.join("~", "data", "s3_tmp"))
    unless File.exists?(@dir)
      FileUtils.mkdir_p(@dir)
    end
    
    AWS::S3::Base.establish_connection!(:access_key_id => args[:key], :secret_access_key => args[:secret])
  end
  
  def download(path, uri)
    
    retries     = 0
    uri         = uri.gsub(/^\//, "")
    object      = AWS::S3::S3Object.find(uri, @src)
    
    puts "downloading #{uri} to #{path}"
    
    unless @dry
      File.open(path, "w") do |file|
        
        begin
          object.value { |data| file.write(data) }
        
        rescue Object => e
        
          if retries < @max_retries
            sleep_seconds = 1
            puts "Caught exception. Sleeping for #{sleep_seconds} seconds and retrying."
            sleep sleep_seconds
            retries += 1
            retry
          else
            puts "giving up."
          end
        end
        
      end
    end
    
    return object
  end
  
  def upload(path, object)
    
    return unless File.exists?(path)
    
    sleep_seconds = 1
    retries       = 0
    
    puts "uploading #{path} to #{object.key} with content-type #{object.content_type}"
    
    unless @dry
      while retries < @max_retries
        response = AWS::S3::S3Object.store(object.key, File.open(path), @dest, :access => :public_read, :content_type => object.content_type)
        break if response.success?
        
        puts "Unable to store file on s3. Sleeping for #{sleep_seconds} seconds"
        sleep sleep_seconds
        retries += 1
      end
      
      puts "removing #{path}..."
      File.unlink(path)
    end
  end
  
  def run
    total = `wc -l #{@path}`.split(/\s+/).first.to_i
    count = 0
    
    File.open(@path).each_line do |line|
      
      fields = line.split(/\t/)
      path   = File.join(@dir, fields[1])
      uri    = fields[2]
      
      upload(path, download(path, uri))
      
      puts "#{count += 1} of #{total} finished"
    end
  end
end

copier = S3Copier.new(
  {
    :path   => ,
    :src    => "", 
    :dest   => ,
    :key    => "",
    :secret => "",
    :dry    => false
  }
)

puts "starting..."
copier.run
puts "done."