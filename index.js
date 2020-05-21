const config = require("config");
const http = require('http')
const kafka = require('kafka-node');
const Bowser = require("bowser");
const utmExtractor = require("utm-extractor").Utm;

const port = config.port || 3012
const host = config.host || "127.0.0.1"
const topic = config.kafka_topic || "revengine_events";

const Producer = kafka.Producer;
const client = new kafka.KafkaClient({ kafkaHost: config.kafka_server || "localhost:9092" });
const producer = new Producer(client);

// Ensure we have the topic created
client.createTopics([
    {
        topic,
        partitions: config.kafka_partitions || 1,
        replicationFactor: config.kafka_replication_factor || 1
    }
], (err, result) => {
    if (err) {
        console.error("Error creating topic");
        console.error(err);
        return process.exit(1);
    }
    // console.log(result);
});

var hits = 0;
var loops = 0;
var cache = [];

class AnalyticsCollect {
    constructor() {
    }

    async hit(req, res) {
        try {
            // console.log(req);
            let body = [];
            req.on('data', (chunk) => {
                body.push(chunk);
            }).on('end', async () => {
                body = Buffer.concat(body).toString();
                let data = null;
                try {
                    data = JSON.parse(body);
                    console.log(data);
                    res.writeHead(200, {
                        'Content-Type': 'text/json',
                        'Access-Control-Allow-Origin': '*',
                        'X-Powered-By': 'revengine'
                    });
                    res.write(JSON.stringify({
                        status: "ok"
                    }))
                    res.end();
                    
                } catch(err) {
                    res.writeHead(500, {
                        'Content-Type': 'text/json',
                        'Access-Control-Allow-Origin': '*',
                        'X-Powered-By': 'revengine'
                    });
                    res.write(JSON.stringify({
                        status: "error",
                        error: JSON.stringify(err)
                    }))
                    res.end();
                }
                try {
                    if (!data) throw "No data";
                    if (!data.action) throw "No action";
                    let index = null;
                    if (data.action === "pageview") {
                        index = "pageviews";
                    }
                    if (!index) throw `No index found for action ${data.action}`;
                    const ua = Bowser.parse(data.user_agent);
                    let utm = {};
                    try {
                        utm = new utmExtractor(data.query_string).get();
                    } catch(err) {
                        // All good
                    }
                    const esdata = {
                        index,
                        action: "hit",
                        article_id: data.post_id,
                        author_id: data.post_author,
                        derived_ua_browser: ua.browser.name,
                        derived_ua_browser_version: ua.browser.version,
                        derived_ua_device: ua.platform.type,
                        derived_ua_os: ua.os.name,
                        derived_ua_os_version: ua.os.version,
                        derived_ua_platform: ua.platform.vendor,
                        referer: data.referer,
                        signed_in: !!(data.user_id),
                        tags: data.post_tags,
                        sections: data.post_sections,
                        time: new Date(),
                        url: data.raw_uri,
                        user_agent: data.user_agent,
                        user_id: data.user_id,
                        utm_medium: utm.utm_medium,
                        utm_campaign: utm.utm_campaign,
                        utm_content: utm.utm_content,
                        utm_source: utm.utm_source,
                        utm_term: utm.utm_term,
                        browser_id: data.browser_id,
                    }
                    console.log({ esdata });
                    let sendResult = await new Promise((resolve, reject) => {
                        producer.send([{
                            topic,
                            messages: JSON.stringify(esdata),
                        }], (err, data) => {
                            if (err) return reject(err);
                            return resolve(data);
                        });
                    });
                    console.log(sendResult);
                } catch(err) {
                    console.error(err);
                }
            });
            
        } catch (err) {
            console.error(new Date(), err);
        }
    }
}


const cache_size = config.cache_size || 1000;

const flush = async () => {
    if (cache.length >= cache_size) {
        try {
            if (config.debug) {
                console.log("Cache length:", cache.length);
            }
            const result = await esBulk({ maxRetries: 5, body: cache });
            cache = [];
            if (config.debug) {
                console.log(`Flushed cache, loop ${loops++}, hits ${hits}, items ${result.items.length}`);
                console.log(result);
                console.log(result.items[0]);
            }
        } catch (err) {
            console.error(err);
        }
    }
}

const analytics = new AnalyticsCollect();

http.createServer((req, res) => {
    if (req.url == '/favicon.ico') return;
    analytics.hit(req, res)
    hits++;
}).listen(port, host, () => {
    console.log(new Date(), `Server listening ${host}:${port}`);
});
