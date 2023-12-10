import { jwtDecode } from "jwt-decode";

// There's support for two types of rate-limiting behaviors
enum Behavior {
	Blocking,
	Throttling
}

// Default behavior is Blocking
const DEFAULT_BEHAVIOR: Behavior = Behavior.Blocking;

export interface Env {
	COUNTER: DurableObjectNamespace
}

export default {
	// Example #1
	//
	// Identifier: 		In this example we are using the client's IP address
	//
	// Configuration: 	In this example we read the number of allowed requests and the time window from two requests
	// 					headers. Obviously we don't want clients to specify their own rate-limits, but it's a
	// 					nice & easy way to test this Worker.
	async default(request: Request): Promise<[string, Configuration]> {
		const ident = request.headers.get('cf-connecting-ip') || '127.0.0.1';

		const configuration = new Configuration(
			request.headers.get('_requests'),
			request.headers.get('_per_seconds'),
			Behavior.Blocking
		);

		return [ident, configuration];
	},

	// Example #2
	//
	// Identifier: 		In this example we are expecting a JWT in a request header called 'api-key'. We are identifying
	//					client's by the JWT's subject
	//
	// Configuration: 	Additionally we are reading the number of requests and the time windows from two JWT claims
	//					called 'requests' and 'per_seconds'
	//
	// Example JWT:		{"iss": "Test Issuer", "sub": "Test Subject", "requests": "10", "per_seconds": "10" }
	async jwt(request: Request): Promise<[string, Configuration]> {
		try {
			// Get JWT from request header
			const token = request.headers.get('api-key') || '';

			// Decode Jwt
			const jwt = jwtDecode(token);

			// Rate-Limit based on JWT subject
			const ident = jwt.sub || '';

			// Get Rate-Limit configuration from two JWT claims 'requests' and 'per_seconds'
			const configuration = new Configuration(
				jwt.requests,
				jwt.per_seconds,
				Behavior.Throttling
			);

			return [ident, configuration];
		} catch(ex) {
			// Use default config if we can't decode the JWT
			return await this.default(request);
		}
	},

	// Workers fetch event
	async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
		const setup = await this.jwt(request);

		// Check if rate is exceeded
		const rate_limited = await this.rate_exceeded(setup[0], setup[1], request, env);

		// Return a 429 if the Rate-Limit was exceeded
		if (rate_limited) {
			return new Response('failed', {
				status: 429
			});
		}

		// What to do if the client wasn't rate-limited. In this example we are fetching from the origin.
		return await fetch(request);
	},

	// Returns true if the client exceeded the Rate-Limit, otherwise false
	async rate_exceeded(ident: string, configuration: Configuration, _request: Request, env: Env): Promise<boolean> {
		// Get Durable Object ID from ident
		const id = env.COUNTER.idFromName(ident);

		// Instantiate Durable Object
		const obj = env.COUNTER.get(id)

		// Create a Durable object request, passing the configuration
		let request = new Request(_request.url, _request);
		request.headers.set('_configuration', JSON.stringify(configuration));

		// Get response from Durable Object
		const response = await obj.fetch(request);

		// Return verdict
		return (response.status === 429)
	}
};

export class Counter {
	// DO state
	state: DurableObjectState

	// Specifies the timestamp of the last successful / not-ratelimited request
	last: number

	// Stores a token bucket
	bucket: Bucket

	constructor(state: DurableObjectState, env: Env) {
		this.state = state;

		this.state.blockConcurrencyWhile(async () => {
			// Load timestamp of the last successful request
			this.last = await this.state.storage.get("last") || 0;

			// Load bucket from storage
			const bucket : Bucket | null = await this.state.storage.get("tokens") || null;

			// If there was no bucket we create a new one, otherwise we reconstruct the Bucket class from the
			// storage object.
			this.bucket = (bucket === null) ? new Bucket() : new Bucket(bucket.max, bucket.tokens, bucket.active);
		});
	}

	// Handle HTTP requests from clients.
	async fetch(request: Request): Promise<Response> {
		// Get current time of request
		const time = this.current_time();

		// Get configuration
		const configuration = this.configuration(request);

		// If no Rate-Limit was specified we can return immediately
		if (configuration.requests === null) {
			return this.respond(true, time);
		}

		// Sets the bucket's maximum size
		this.bucket.set_size(configuration.requests);

		// Calculate the number of tokens we can add to the bucket.
		// This is either the maximum number of tokens, in case the bucket hasn't been used yet, or the number
		// of tokens we can re-add to the bucket if a rate-limit threshold per seconds was configured.
		const fill_amount = (!this.bucket.is_active())
			? configuration.requests
			: this.fill_amount(configuration.requests, configuration.per_seconds, time);

		// Fill the bucket, but only if we wouldn't block the request. This is a special condition that applies to
		// Behavior.Blocking only. Check should_block() for more.
		if (!this.should_block(time, configuration)) {
			this.bucket.fill(fill_amount);
		}

		// Take a token from the bucket. If enough tokens are available we'll return a 200.
		// Otherwise, we return a 429 that can be checked from the Worker script
		return (this.bucket.take(false))
			? this.respond(true, time)
			: this.respond(false, time)
	}

	// Returns current timestamp in milliseconds
	current_time(): number {
		return Math.trunc(new Date().getTime());
	}

	// Get Rate-Limit configuration from request
	configuration(request: Request): Configuration {
		let config = request.headers.get("_configuration") || null;

		// Use default configuration if not specified
		if (config === null) {
			return new Configuration();
		}

		// Otherwise parse the configuration
		let parse: Configuration = JSON.parse(config);

		// And make sure we have valid inputs
		return new Configuration(
			parse.requests,
			parse.per_seconds,
			parse.behavior
		);
	}

	// Returns the number of tokens that can be added to the bucket.
	fill_amount(tokens: number, seconds: number | null, time: number): number {
		// If the number of seconds is unspecified, there are no token's to add
		if (seconds === null) {
			return 0;
		}

		// Calculate number of tokens per second this DO can use
		const tpsf = tokens / seconds;

		// Calculate number of full seconds that have passed since the last successful request
		const passed = Math.floor((time - this.last) / 1000);

		// Re-fill bucket
		return Math.floor(tpsf * passed);
	}

	// For Behavior.Blocking, this function returns true if we would block the request, because the client exceeded
	// their Rate-Limit threshold. This function effectively returns false for any request where the difference between
	// 'now' and 'last successful' request is below the configured Rate-Limit time windows. Used in combination
	// with the bucket refill methods, this allows us to simulate a blocking behavior
	should_block(time: number, configuration: Configuration) {
		if (configuration.behavior !== Behavior.Blocking) {
			return false
		}

		if (configuration.per_seconds === null) {
			return false;
		}

		if (this.bucket.take(true)) {
			return false;
		}

		return Math.floor((time - this.last) / 1000) < configuration.per_seconds;
	}

	// Persists last timestamp and bucket
	persist(): void {
		this.state.storage.put("last", this.last);
		this.state.storage.put("tokens", this.bucket);
	}

	// Respond with either a 200 or 429
	respond(success: boolean, time: number): Response {
		let status = (success) ? 200 : 429;

		// Only store last timestamp of a successful request. See fill_amount() for more.
		if (success) {
			this.last = time;
		}

		// Persist data
		this.persist();

		return new Response('', {
			status: status
		});
	}
}

class Bucket {
	// Maximum number of tokens a bucket can take.
	max: number

	// Number of tokens currently available in the bucket.
	tokens: number

	// Specifies if the bucket is active / has been used yet.
	active: boolean

	// Creates a new token bucket
	constructor(max: number = 0, tokens: number = max, active: boolean = false) {
		this.max = max;
		this.tokens = tokens;
		this.active = active;
	}

	// Returns true if that bucket is active
	is_active(): boolean {
		return this.active
	}

	// Set the size of the bucket
	set_size(size: number): void {
		this.max = size;
	}

	// Takes one token from the bucket. If a token was available this function returns true, otherwise false.
	take(pre: boolean): boolean {
		// Bucket is now active
		this.active = true;

		// If the bucket is empty we can return
		if (this.tokens <= 0) {
			return false;
		}

		if (!pre) {
			--this.tokens;
		}
		return true;
	}

	// Fills the bucket with tokens specified by num. The bucket won't fill above the configured maximum size.
	fill(num: number): void {
		this.tokens += Math.min(this.max - this.tokens, num);
	}
}

class Configuration {
	// Specifies the number of requests a client can do
	requests: number | null

	// Specifies a Rate-Limit time window in seconds
	per_seconds: number | null

	// Specifies the Rate-Limit behavior
	behavior: Behavior

	// Creates a new configuration
	constructor(requests: any = null, per_seconds: any = null, behavior: Behavior = DEFAULT_BEHAVIOR) {
		this.requests = Number(requests) || null
		this.per_seconds = Number(per_seconds) || null
		this.behavior = behavior;
	}
}