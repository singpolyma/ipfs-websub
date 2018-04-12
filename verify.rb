require "json"
require "open-uri"
require "redis"
require "securerandom"
require "uri"

def u(s)
	URI.escape(s, Regexp.new("[^#{URI::PATTERN::UNRESERVED}]"))
end

REDIS = Redis.new

def verify_with_subscriber(item)
	puts "Verifying with subscriber"
	challenge = SecureRandom.hex
	callback = URI.parse(item["callback"])
	callback.query += "&hub.mode=#{item["mode"]}&hub.topic=#{u item["topic"]}&hub.challenge=#{challenge}&hub.lease_seconds=#{item["lease"]}"
	challenge2 = begin
		callback.read
	rescue OpenURI::HTTPError => e
		raise e unless e.io.status[0].to_i == 404
		return false
	end

	return false unless challenge == challenge2

	true
end

def subscribe_one_item(item)
	puts "Resolving #{item["ipns"]}"
	resolved = JSON.parse(open("http://localhost:5001/api/v0/name/resolve?r&arg=#{u item["ipns"]}").read)["Path"]

	unless resolved
		puts "IPNS resolve failed, notifying subscriber"
		callback = URI.parse(item["callback"])
		callback.query += "&hub.mode=denied&hub.topic=#{u item["topic"]}&hub.reason=Not%20a%20valid%20IPNS%20name."
		callback.read
		return
	end

	return unless verify_with_subscriber(item)

	REDIS.zadd(item["ipns"], Time.now.to_i + item["lease"], item["callback"])
	unless REDIS.hexists("last_resolved_to", item["ipns"])
		REDIS.hset("last_resolved_to", item["ipns"], resolved)
	end
end

def unsubscribe_one_item(item)
	return unless verify_with_subscriber(item)
	REDIS.zrem(item["ipns"], item["callback"])
end

def one_item(raw_item)
	item = JSON.parse(raw_item)

	case item["mode"]
	when "subscribe"
		subscribe_one_item(item)
	when "unsubscribe"
		unsubscribe_one_item(item)
	else
		raise "Unknown mode"
	end
rescue => e
	warn "Error: #{e.inspect} -- writing to verify_errors"
	item["errors"] ||= []
	item["errors"] << e.inspect
	REDIS.lpush("verify_errors", JSON.dump(item))
end

loop do
	raw_item = REDIS.lindex("verifying", 0) || REDIS.brpoplpush("to_verify", "verifying")
	puts "Got item #{raw_item}"

	one_item(raw_item)

	REDIS.lrem("verifying", 1, raw_item)
	puts "Done item"
end
