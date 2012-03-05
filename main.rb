require "em-resolv-replace"
require "em-http-request"
require "em-redis"

class Crawler

    def initialize(seed_links)
        @compiled_regexp = Regexp.new(/href.?=.?["']([^\/].*?)["']/i)
        tmp_con = EM::Protocols::Redis.connect({:host => '127.0.0.1', :port => 6379, :db => 0})
        tmp_con.scard('links_to_crawl') do |link_amount|
            seed_links.each {|link| tmp_con.sadd('links_to_crawl', link)} if link_amount.to_i == 0
        end
    end

    def start_fresh_crawl(redis_connection = nil)
        redis_connection = EM::Protocols::Redis.connect({:host => '127.0.0.1', :port => 6379, :db => 0}) unless redis_connection
        redis_connection.spop('links_to_crawl') do |link|
            if link
                crawl_url(link, redis_connection)
            else
                puts "Queue empty, trying again in 10 seconds"
                EventMachine::Timer.new(10){ start_fresh_crawl(redis_connection) }
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

    def crawl_url(url, redis_connection)
        redis_connection.sadd('visited_links', url)
        grab_html(url, redis_connection) do |html_data|
            links = extract_internal_links(url, html_data)

            links.each {|link| 
                redis_connection.sismember('visited_links', link) do |is_member|
                    redis_connection.sadd('links_to_crawl', link) unless is_member
                end
            }
            
            title = extract_title(html_data)
            redis_connection.scard('links_to_crawl') do |queue_size|
                redis_connection.scard('visited_links') do |visited_size|
                   puts "[Crawled: #{visited_size} | Queue size: #{queue_size} | Crawled #{url}: #{title.inspect}"
                   start_fresh_crawl(redis_connection)
               end
           end
       end
   end

   def grab_html(url, redis_connection)
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
            next_link = @links_to_crawl.pop
            EM.next_tick { crawl_url(next_link, redis_connection) if next_link}
        end

    rescue StandardError => e
        puts "Got an Exception: #{e.message}"
        start_fresh_crawl(redis_connection)
    end
end
end


$crawled_so_far = 0
#Launch the reactor in its own thread
reactor_thread = Thread.new {EventMachine.run}
sleep 1 until EventMachine.reactor_running?

initial_seed_links = ['http://www.engadget.com/', 'http://techcrunch.com/']
parallelism = 20
my_crawler = Crawler.new(initial_seed_links)
parallelism.times do |i|
    puts "Crawler #{i+1}/#{parallelism} started"
    my_crawler.start_fresh_crawl
end

reactor_thread.join