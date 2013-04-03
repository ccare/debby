var express = require('express')
var AWS = require('aws-sdk');
var Str = require('string');
var s3Lib = require('s3');
var exec = require('child_process').exec, child;
var fs = require('fs');
var knox = require('knox');
var Kat = require('kat');
var zlib = require('zlib');
var async = require('async');
var crypto = require('crypto');
var spawn = require('child_process').spawn;

AWS.config.loadFromPath('./config.json');
var s3 = new AWS.S3();

var app = require('express')(),
    server = require('http').createServer(app)

var BUCKET = 'debby'
var REGION = 'eu-west-1'
var REPO_ORIGIN = 'ccare'
var REPO_LABEL = 'ccare'
var REPO_ARCH = 'amd64'
var REPO_DESCRIPTION = 'ccare repo'

var knoxClient = knox.createClient({
    key: AWS.config.credentials.accessKeyId
  , secret: AWS.config.credentials.secretAccessKey
  , bucket: BUCKET
  , region: REGION
});

var s3Client = s3Lib.fromKnox(knoxClient);


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



