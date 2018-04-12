require "json"
require "rack"
require "redis"
require "uri"

MIN_LEASE = 60*60*24
DEFAULT_LEASE = 60*60*24*10
MAX_LEASE = 60*60*24*60

REDIS = Redis.new

def err(s)
	[400, { "Content-Type" => "text/plain" }, [s, "\n"]]
end

run ->(env) {
	unless env["REQUEST_METHOD"] == "POST"
		return err("Subscribe using POST with hub.callback, etc")
	end

	req = Rack::Request.new(env)

	unless ["subscribe", "unsubscribe"].include?(req.POST["hub.mode"])
		return err("Unknown hub.mode")
	end

	topic = begin
		URI.parse(req.POST["hub.topic"])
	rescue
		return err("hub.topic not a valid URI")
	end

	callback = begin
		URI.parse(req.POST["hub.callback"])
	rescue
		return err("hub.callback not a valid URI")
	end

	unless ["http", "https"].include?(callback.scheme.downcase)
		return err("hub.callback not HTTP(S)")
	end

	if topic.query || topic.fragment
		return err("hub.topic with query or fragment not supported")
	end

	ipns = if topic.path.start_with?("/ipns/")
		topic.path
	else
		"/ipns/#{topic.host.downcase}#{topic.path}"
	end

	REDIS.lpush(
		"to_verify",
		JSON.dump(
			mode: req.POST["hub.mode"],
			topic: req.POST["hub.topic"], # Need literally to pass back for verification
			callback: callback,
			lease: [[req.POST["hub.lease_seconds"].to_i, MIN_LEASE].max, MAX_LEASE].min,
			secret: req.POST["hub.secret"],
			ipns: ipns
		)
	)

	[202, { "Content-Type" => "text/plain" }, ["Request enqueued\n"]]
}
