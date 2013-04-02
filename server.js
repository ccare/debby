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

AWS.config.loadFromPath('./config.json');
var s3 = new AWS.S3();

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

var s3Client = s3Lib.fromKnox(knoxClient);


var Indexer = require('./lib/indexer').Indexer


new Indexer(knoxClient).reIndex();





// function createMetaAndUpload(tmpFolder, localDebFile, meta) {
// 	// fork process, catch output
// 	exec('dpkg-scanpackages pool >' + meta,
// 		{ encoding: 'utf8',
// 			  timeout: 5000,
// 			  killSignal: 'SIGTERM',
// 			  cwd: tmpFolder },
// 	  function(error, stdout, stderr) {
// 	    console.log('stdout: ' + stdout);
// 	    console.log('stderr: ' + stderr);
// 	    if (error !== null) {
// 	      console.log('exec error: ' + error);
// 	    }
// 		uploadMeta(tmpFolder + "/" + meta, "/meta/" + meta)
// 	});

// }


// function report(r, callback) {
// 	console.log("WATERFALL DONE - %j", r)
// }

// // s3.client.listObjects( 
// // 	{ 'Bucket': BUCKET, 'Prefix': 'deb/'},
// // 	onSuccess(function(response) {
// // 		var debFileNames = toNameList(response.Contents, 'deb/', '.deb')
// // 		findMissingMetaFiles(debFileNames)
// // 	})
// // )





// function findMissingMetaFiles(debFileNames) {
// 	s3.client.listObjects( 
// 		{ 'Bucket': BUCKET, 'Prefix': 'meta/'},
// 		onSuccess(function(response) {
// 			var metaFileNames = toNameList(response.Contents, 'meta/', '.meta')
// 			compareMetaAndDeb(debFileNames, metaFileNames)
// 		})
// 	)
// }

// function compareMetaAndDeb(debs, metas) {
// 	console.log("Debs: %j", debs);
// 	console.log("Metas: %j", metas);
// 	var missing = []
// 	debs.forEach(function(deb, idx) {
// 		var expectedMeta = deb + ".meta";
// 		if (metas.indexOf(expectedMeta) < 0) {
// 			missing.push( { 'deb': deb, 'meta': expectedMeta } )
// 		}
// 	})
// 	console.log("Missing: %j", missing);
// 	missing.forEach(function(missingMeta) {
// 		buildMetadataFile(missingMeta.deb, missingMeta.meta)
// 	})
// }

// function buildMetadataFile(deb, meta) {
// 	console.log("%s -> %s", deb, meta);
// 	var tmpFolder = "/tmp/debby__" + deb + "__" + Date.now();
// 	console.log("Mkdir %s", tmpFolder);
// 	fs.mkdirSync(tmpFolder);
// 	fs.mkdirSync(tmpFolder + "/pool");
// 	var tmpFile = tmpFolder + "/pool/" + deb;
// 	var downloader = s3Client.download("/deb/" + deb, tmpFile);
// 	downloader.on('error', function(err) {
// 	  console.error("unable to download:", err.stack);
// 	});
// 	downloader.on('end', function() {
// 	  console.log("done");
// 	  createMetaAndUpload(tmpFolder, tmpFile, meta);
// 	});
// }

// function createMetaAndUpload(tmpFolder, localDebFile, meta) {
// 	// fork process, catch output
// 	exec('dpkg-scanpackages pool >' + meta,
// 		{ encoding: 'utf8',
// 			  timeout: 5000,
// 			  killSignal: 'SIGTERM',
// 			  cwd: tmpFolder },
// 	  function(error, stdout, stderr) {
// 	    console.log('stdout: ' + stdout);
// 	    console.log('stderr: ' + stderr);
// 	    if (error !== null) {
// 	      console.log('exec error: ' + error);
// 	    }
// 		uploadMeta(tmpFolder + "/" + meta, "/meta/" + meta)
// 	});

// }

// function uploadMeta(localMetaFile, meta) {
// 	var uploader = s3Client.upload(localMetaFile, meta);
// 	uploader.on('error', function(err) {
// 	  console.error("unable to upload:", err.stack);
// 	});
// 	uploader.on('end', function() {
// 	  console.log("done");
// 	});
// }

function onSuccess(fn) {
	return function(err, data) {
		if (err != undefined || err != null) { 
			console.log("ERROR: %j", err);
		} else {
			fn(data)
		}
	};
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
	 buildRelease(res)
})

app.get('/Release2', function(req, res) {
    res.header('content-type', 'text/plain')
	buildRelease2(function(contents) {
		res.status(200).send(contents)
	})
})

app.get('/Release2.gpg', function(req, res) {
    res.header('content-type', 'text/plain')
	buildRelease2(function(contents, callback) {
		var gpb = spawn('gpg', ['-v', '--output', '-', '-u', 'Deb Repo', '-ba']);
      	gpb.stdout.pipe(res)
	    gpb.stdin.write(contents) 
	    gpb.stdin.end();
	})
})


function buildReleaseText(md5, size) {
	return "Origin: ccare\n"
	+"Label: ccare\n"
	+"Architectures: amd64\n"
	+"Description: ccare repo\n"
	+"MD5Sum:\n"
	+" " + md5 + "      " + size + " Packages\n"
}

function sendRelease(md5, size, res) {
	var val = buildReleaseText(md5, size);
	res.status(200).send(val)
}


var spawn = require('child_process').spawn;



app.get('/Release.gpg', function(req, hres) {
        hres.status(200) //.sendfile('./Release.gpg')
        s3.client.listObjects( 
			{ 'Bucket': BUCKET, 'Prefix': 'meta/'},
			onSuccess(function(response) {
				var metaFileNames = toNameList(response.Contents, '', '.meta')
				streamAndHashMetas(metaFileNames, 0, null, 0, function(err, md5, size) {
					var gpb = spawn('gpg', ['-v', '--output', '-', '-u', 'Deb Repo', '-ba']);
      			    gpb.stdout.pipe(hres)
					var val = buildReleaseText(md5, size);
      			    gpb.stdin.write(new Buffer(val))
      			    gpb.stdin.end();
   				})
			})
		)
	})

app.get('/pool/:deb', function(req, hres) {
	var deb = req.params.deb;
	if (!Str(deb).endsWith('.deb')) {
		hres.send(404, "Not found.\n")
	}

    	knoxClient.get('/deb/' + deb)
        .on('response', function(s3Res) {
        	console.log("streaming out deb");
			s3Res.pipe(hres);
		}).end()
		})

server.listen(3000)

function buildRelease(res, callback) {
	s3.client.listObjects( 
		{ 'Bucket': BUCKET, 'Prefix': 'meta/'},
		onSuccess(function(response) {
			var metaFileNames = toNameList(response.Contents, '', '.meta')
			streamAndHashMetas(metaFileNames, 0, null, 0, function(err, md5, size) {
				sendRelease(md5, size, res)
			})
		})
	)
}



function buildRelease2(outerCallback) {
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

function computeDigest(callback) {
	var md5sum = crypto.createHash('md5');
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


function streamAndHashMetas2(metas, idx, md5sum, size, callback) {
	if (md5sum == null) {
		md5sum = crypto.createHash('md5');
	}
	if (size == null) {
		size = 0;
	}
	var meta = metas[idx]
	var isLast = metas.length == (idx+1)
	knoxClient.get(meta)
        .on('response', function(s3Res) {
        	console.log("HASHING STREAM");
			s3Res.on('data', function(d) {
			  md5sum.update(d);
			  size = size + d.length
			});
			s3Res.on('end', function() {
			  	if ( ! isLast) {
					streamAndHashMetas(metas, idx+1, md5sum, size, callback)
				} else {
					var d = md5sum.digest('hex');
  					console.log('MD5: ' + d);
  					callback(null, d, size)
				}
			});			
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
			onSuccess(function(response) {
				var objectNames = toNameList(response.Contents, '', extension)
				callback(null, objectNames)
			})
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

function streamAndHashMetas(metas, idx, md5sum, size, callback) {
	if (md5sum == null) {
		md5sum = crypto.createHash('md5');
	}
	if (size == null) {
		size = 0;
	}
	var meta = metas[idx]
	var isLast = metas.length == (idx+1)
	knoxClient.get(meta)
        .on('response', function(s3Res) {
        	console.log("HASHING STREAM");
			s3Res.on('data', function(d) {
			  md5sum.update(d);
			  size = size + d.length
			});
			s3Res.on('end', function() {
			  	if ( ! isLast) {
					streamAndHashMetas(metas, idx+1, md5sum, size, callback)
				} else {
					var d = md5sum.digest('hex');
  					console.log('MD5: ' + d);
  					callback(null, d, size)
				}
			});			
		}).end()
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