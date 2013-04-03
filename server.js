var express = require('express')
var AWS = require('aws-sdk');
var Str = require('string');
var knox = require('knox');
var zlib = require('zlib');

AWS.config.loadFromPath('./config.json');

var app = require('express')(),
    server = require('http').createServer(app)

var BUCKET = 'debby'
var REGION = 'eu-west-1'

var knoxClient = knox.createClient({
    key: AWS.config.credentials.accessKeyId
  , secret: AWS.config.credentials.secretAccessKey
  , bucket: BUCKET
  , region: REGION
});


var Indexer = require('./lib/indexer').Indexer
var indexer = new Indexer(knoxClient)


indexer.updateIndexes();


app.get('/pool/:deb', function(req, res) {
	var deb = req.params.deb;
	if (!Str(deb).endsWith('.deb')) {
		hres.send(404, "Not found.\n")
	}
	streamObject('/deb/' + deb, res) 
})

app.get('/Packages', function(req, res) {
    res.header('content-type', 'text/plain')
	streamObject('/repo/Packages', res) 
})

app.get('/Packages.gz', function(req, res) {
	res.header('content-type', 'application/x-gzip')
	var out = zlib.createGzip();
	streamObject('/repo/Release', out) 
	out.pipe(res)
})

app.get('/Release', function(req, res) {
    res.header('content-type', 'text/plain')
	streamObject('/repo/Release', res) 
})

app.get('/Release.gpg', function(req, res) {
    res.header('content-type', 'text/plain')
	streamObject('/repo/Release.gpg', res) 
})

app.post('/updateIndexes', function(req, res) {
	indexer.updateIndexes();
    res.send("OK")
})

app.post('/reindex', function(req, res) {
	indexer.reIndex();
    res.send("OK")
})

function streamObject(object, out) {
	knoxClient.get(object)
	    .on('response', function(res) {
	    	console.log("streaming out object %s", object);
			res.pipe(out);
		}).end()
}




server.listen(3000)



