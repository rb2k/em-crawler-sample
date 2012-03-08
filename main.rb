require "rubygems"
require "bundler/setup"
require 'bundler/setup'
require "em-resolv-replace"
require "em-http-request"
require "em-redis"
require 'addressable/uri'

class Crawler

    def initialize(seed_links, redis_connection)
        #Let's compile the regexp once rather than compiling it inline everytime
        @compiled_regexp = Regexp.new(/href.?=.?["']([^\/].*?)["']/i)
        @redis_connection = redis_connection
        tmp_con = EM::Protocols::Redis.connect({:host => '127.0.0.1', :port => 6379, :db => 0})
        tmp_con.scard('links_to_crawl') do |link_amount|
            seed_links.each {|link| tmp_con.sadd('links_to_crawl', link)} if link_amount.to_i == 0
        end
        #An array with the timestamp of the last crawl for a certain domain
        #This will also be in the future for planned requests
        @domain_crawl_timestamp = {}
        #force a distance of 2 seconds between http requests (per domain)
    end

    def random_crawl_delay
        #a random delay from 0 to 2 seconds, floating point precision
        rand(200) / 100.0
    end

    def start_fresh_crawl()
        @redis_connection.spop('links_to_crawl') do |link|
            if link
                link_host = Addressable::URI.parse(link).host
                last_request_for_domain = @domain_crawl_timestamp[link_host].to_f
                current_delay = random_crawl_delay()
                if (last_request_for_domain + current_delay) < Time.now.to_f
                    #The last HTTP request to this domain has been longer ago than our crawl delay
                    #So we can crawl it immediately
                    @domain_crawl_timestamp[link_host] = Time.now.to_f
                    crawl_url(link)
                else
                    #The last HTTP request to this domain is still within our waiting period
                    #We will launch the request in the future
                    wait_for = last_request_for_domain + current_delay - Time.now.to_f
                    #Save the point in time when we will crawl the domain again.
                    @domain_crawl_timestamp[link_host] = Time.now.to_f + wait_for.to_f
                    EventMachine::Timer.new(wait_for){ crawl_url(link) }
                end
                
            else
                puts "Queue empty, trying again in 10 seconds"
                EventMachine::Timer.new(10){ start_fresh_crawl() }
            end
        end
    end

    private

    def extract_internal_links(url, html_data)
        current_domain = Addressable::URI.parse(url).host rescue nil
        data = html_data.scan(@compiled_regexp).flatten.map{|item| item.to_s.strip.downcase}.uniq
        data.select! do |link|
            uri = Addressable::URI.parse(link) rescue nil
            uri && uri.host == current_domain
        end
        data.map{|link| uri = Addressable::URI.parse(link); uri.fragment = nil; uri.query = nil; uri.to_s}
    end

    def extract_title(html)
        html.match(/<title>(.*)<\/title>/)[1] rescue nil
    end

    def crawl_url(url)
        @redis_connection.sadd('visited_links', url)
        grab_html(url) do |html_data|
            links = extract_internal_links(url, html_data)

            links.each {|link| 
                @redis_connection.sismember('visited_links', link) do |is_member|
                    @redis_connection.sadd('links_to_crawl', link) unless is_member
                end
            }
            
            title = extract_title(html_data)
            @redis_connection.scard('links_to_crawl') do |queue_size|
                @redis_connection.scard('visited_links') do |visited_size|
                   puts "[Crawled: #{visited_size} | Queue size: #{queue_size} | Crawled #{url}: #{title.inspect}"
                   start_fresh_crawl()
               end
           end
       end
   end

   def grab_html(url)
    begin
        request_options = {
            :redirects => 5,
            :keepalive => true,
            :head => {'user-agent'=> '"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_2) AppleWebKit/535.7 (KHTML, like Gecko) Chrome/16.0.912.63 Safari/535.7"'}
        }
        http = EventMachine::HttpRequest.new(url).get(request_options)
        http.callback do 
            yield(http.response)
        end
        http.errback do 
            puts "HTTP Error for #{url}: #{http.response_header.status}"
            start_fresh_crawl()
        end

    rescue StandardError => e
        puts "Got an Exception: #{e.message}"
        start_fresh_crawl()
    end
end
end


$crawled_so_far = 0
#Launch the reactor in its own thread
reactor_thread = Thread.new {EventMachine.run}
sleep 1 until EventMachine.reactor_running?

initial_seed_links = ['http://www.engadget.com/', 'http://techcrunch.com/']
#The amount of parallel transmissions that run at once
parallelism = 20
redis_connection = EM::Protocols::Redis.connect({:host => '127.0.0.1', :port => 6379, :db => 0})
my_crawler = Crawler.new(initial_seed_links, redis_connection)
parallelism.times do |i|
    puts "Crawler #{i+1}/#{parallelism} started"
    my_crawler.start_fresh_crawl()
end

reactor_thread.join