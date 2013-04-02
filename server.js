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


new Indexer(knoxClient).reIndex();


function buildReleaseText(md5, size) {
	return "Origin: " + REPO_ORIGIN + "\n"
	+"Label: " + REPO_LABEL + "\n"
	+"Architectures: " + REPO_ARCH + "\n"
	+"Description: " + REPO_DESCRIPTION + "\n"
	+"MD5Sum:\n"
	+" " + md5 + "      " + size + " Packages\n"
}









app.get('/Packages', function(req, res) {
    res.header('content-type', 'text/plain')
	buildPackageBody(res)
})

app.get('/Packages.gz', function(req, res) {
	res.header('content-type', 'application/x-gzip')
	var out = zlib.createGzip();
	buildPackageBody(out)
	out.pipe(res)
})

app.get('/Release', function(req, res) {
    res.header('content-type', 'text/plain')
	buildRelease(function(contents) {
		res.status(200).send(contents)
	})
})

app.get('/Release.gpg', function(req, res) {
    res.header('content-type', 'text/plain')
	buildRelease(function(contents, callback) {
		var gpb = spawn('gpg', ['-v', '--output', '-', '-u', 'Deb Repo', '-ba']);
      	gpb.stdout.pipe(res)
	    gpb.stdin.write(contents) 
	    gpb.stdin.end();
	})
})

app.get('/pool/:deb', function(req, res) {
	var deb = req.params.deb;
	if (!Str(deb).endsWith('.deb')) {
		hres.send(404, "Not found.\n")
	}
	streamObject('/deb/' + deb, res) 
})

function streamObject(object, out) {
	knoxClient.get(object)
	    .on('response', function(res) {
	    	console.log("streaming out deb");
			res.pipe(out);
		}).end()
}

function buildPackageBody(out) {
	async.waterfall([
		listObjects(BUCKET, 'meta/', '.meta'),
		foreachObject(concatObjectToStream(out))
	], 
	streamEnd(out)
	);
}


function buildRelease(outerCallback) {
	async.waterfall([
		listObjects(BUCKET, 'meta/', '.meta'),
		function(fileNames, callback) {
			var md5sum = crypto.createHash('md5');
			var size = 0;
			async.eachSeries(fileNames, 
				function(item, cb) {
					knoxClient.get(item)
       				.on('response', function(s3Res) {
        				s3Res.on('data', function(d) {
			  				md5sum.update(d);
			  				size = size + d.length
						});
						s3Res.on('end', function() {
							cb();
						})
					}).end()
       			},
       			function(err) {
       				var d = md5sum.digest('hex');
  					callback(null, buildReleaseText(d, size))
       			}
			);			
		},
		outerCallback
	]);
}



















server.listen(3000)






function foreachObject(writer) {
	return function(fileNames, callback) {
		async.eachSeries(fileNames, 
			writer, 
			function(err) {
				if (err != null) {
					callback(err, null)
				}
				callback()
			})
	}
}

function streamEnd(out) {
	return function(err) {
		out.end()
	}
}

function listObjects(bucket, prefix, extension) {
	return function(callback) {
		s3.client.listObjects( 
			{ 'Bucket': bucket, 'Prefix': prefix},
			function(err, response) {
				if (err != null) {
					callback(err, null)
				} else {
					var objectNames = toNameList(response.Contents, '', extension)
					callback(null, objectNames)
				}
			}
		)
	}
}

function concatObjectToStream(out) {
	return function(object, callback) {
		knoxClient.get(object)
			.on('response', function(s3Res) {
				s3Res.pipe(out, { end: false });
				s3Res.on('end', function() {
					callback();        				
				})
			})
			.on('error', function(err) {
				callback(err);
			}).end()
	}
}


function toNameList(objects, prefix, extn) {
		var names = []
		objects.forEach(function(item, idx) {
			var name = item.Key;
			if (Str(name).endsWith(extn)) {
				names.push(name.replace(prefix, ''))
			}
		})
		return names;
	}