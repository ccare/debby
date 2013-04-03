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
var s3 = new AWS.S3();
var crypto = require('crypto');
var spawn = require('child_process').spawn;

function Indexer(knoxClient) {

	var BUCKET = 'debby'

	var s3Client = s3Lib.fromKnox(knoxClient);

	this.reIndex = function(callback) {
		async.waterfall([
		    listObjects(BUCKET, 'repo/', ''),
		    function(metas, cb) {
		    	console.log(metas);
		    	knoxClient.deleteMultiple(metas, function(err, res) {
		    		cb();
		    	})
		    },
			this.updateIndexes
		], callback );
	}


	this.updateIndexes = function(callback) {
		async.waterfall([
			listDebsAndMetas,
			findMissingMetadata,
			indexItems,
			rebuildPackagesFile,
			rebuildReleaseFile,
			rebuildReleaseGpgFile,
			deleteTempDirs
		], callback );
	}

	function listDebsAndMetas(callback) {
		async.parallel({
		    debArchives: listObjects(BUCKET, 'deb/', '.deb', 'deb/'),
		    metaFiles: listObjects(BUCKET, 'meta/', '.meta', 'meta/'),
		    repoFiles: listObjects(BUCKET, 'repo/', '', 'repo/')
		}, callback);
	}

	function findMissingMetadata(currentFiles, callback) {
		var debArchives = currentFiles.debArchives
		var metaFiles = currentFiles.metaFiles
		var repoFiles = currentFiles.repoFiles
		var missing = []

		var allRepoFilesPresent = (repoFiles.indexOf('Packages') >= 0)
			&& (repoFiles.indexOf('Release') >= 0)
			&& (repoFiles.indexOf('Release.gpg') >= 0)

		debArchives.forEach(function(deb, idx) {
			var expectedMeta = deb + ".meta";
			if (metaFiles.indexOf(expectedMeta) < 0) {
				missing.push( { 'deb': deb, 'meta': expectedMeta } )
			}
		})

		if (missing.length == 0 && allRepoFilesPresent) {
			console.log("Indexes up to date. Nothing to do.");
			callback("Nothing to do")
		} else {
			console.log("Continue to build indexes");
			callback(null, missing)
		}
	}


	function indexItems(items, callback) {
		async.each(items, indexItem, callback)
	}
	function deleteTempDirs(callback) {
		console.log("Deleting tmp dirs");
		exec('rm -rf debby__*', 
		{
			encoding: 'utf8',
			timeout: 5000,
			killSignal: 'SIGTERM',
			cwd: '/tmp'
		}, function(error, stdout, stderr) {
			callback()
		})	
	}

	function indexItem(item, callback) {
		console.log("Index %s -> %s", item.deb, item.meta)

		var deb = item.deb
		var meta = item.meta
		var tmpFolder = "/tmp/debby__" + deb + "__" + Date.now();
		var poolFolder = tmpFolder + "/pool";
		var packageFile = tmpFolder + "/" + meta;
		var debFile = poolFolder + "/" + deb;
		var debObject = "/deb/" + deb;
		var metaObject = "/meta/" + meta;

		async.series([
	    	makeTempDir(tmpFolder),
	    	makeTempDir(poolFolder),
	    	download(debObject, debFile),
	    	buildMetaFile(tmpFolder, debFile, meta),
	    	upload(packageFile, metaObject)
		],
		callback);	
	}

	function rebuildPackagesFile(callback) {
		var tmpFolder = "/tmp/debby__repoInfo__" + Date.now();
		var packagesFile = tmpFolder + "/Packages";
		var packagesObject = "/repo/Packages";

		async.series([
	    	makeTempDir(tmpFolder),
	    	function(cb) {
	    		var fileStream = fs.createWriteStream(packagesFile);
	    		buildPackageBody(fileStream, function() {
	    			cb()
	    		});
	    	},
	    	upload(packagesFile, packagesObject)
		],
		function(err) {
			callback()
		});
	}

	function rebuildReleaseFile(callback) {
		var releaseObject = "/repo/Release";

		buildRelease(function(contents) {
			var req = knoxClient.put(releaseObject, {
			    'Content-Length': contents.length
			  , 'Content-Type': 'text/plain'
			});
			req.on('response', function(res){
  				if (200 == res.statusCode) {
    				console.log('saved to %s', req.url);
    				callback();
  				}
			});
			req.end(contents);
		})
	}

	function rebuildReleaseGpgFile(callback) {
		
		var tmpFolder = "/tmp/debby__repoInfo__" + Date.now();
		var file = tmpFolder + "/Release.gpg";
		var object = "/repo/Release.gpg";

		async.series([
	    	makeTempDir(tmpFolder),
	    	function(cb) {
	    		var fileStream = fs.createWriteStream(file);
	    		buildRelease(function(contents) {
					var gpb = spawn('gpg', ['-v', '--output', '-', '-u', 'Deb Repo', '-ba']);
			      	gpb.stdout.pipe(fileStream)
				    gpb.stdin.write(contents) 
				    gpb.stdin.end();
					gpb.on('close', function() {
						cb()
					})
				})
	    	},
	    	upload(file, object)
		],
		function(err) {
			callback()
		});
	}

	function download(object, localFile) {
		return function(callback) {
			var downloader = s3Client.download(object, localFile);
			downloader.on('error', function(err) {
			  console.error("unable to download:", err.stack);
			  callback(err, null)
			});
			downloader.on('end', function() {
			  callback(null, {})
			});
		}
	}
	function upload(localFile, object) {
		return function(callback) {
			var uploader = s3Client.upload(localFile, object);
			uploader.on('error', function(err) {
			  console.error("unable to upload:", err.stack);
			  callback(err, null)
			});
			uploader.on('end', function() {
			  callback(null, {});
			});
		}
	}



	function makeTempDir(dir) {
		return function(callback) {
			console.log("creating " + dir);
			fs.mkdirSync(dir);
			callback(null, {});
		}
	}









	function buildMetaFile(tmpFolder, localDebFile, meta) {
		return function(callback) {
			exec('dpkg-scanpackages pool >' + meta,
				{
					encoding: 'utf8',
					timeout: 5000,
					killSignal: 'SIGTERM',
					cwd: tmpFolder
				},
			  	function(error, stdout, stderr) {
			    	console.log('stdout: ' + stdout);
			    	console.log('stderr: ' + stderr);
			    	if (error !== null) {
			    	  	console.log('exec error: ' + error);
			    	}
					callback(null, meta)
				}
			);
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


function listObjects(bucket, prefix, extension, rootPath) {
	rootPath = (rootPath == null) ? '' : rootPath;
	return function(callback) {
		knoxClient.list({ prefix: prefix }, function(err, response)  {
				if (err != null) {
					callback(err, null)
				} else {
					var objectNames = toNameList(response.Contents, rootPath, extension)
					callback(null, objectNames)
				}
			}
		)
	}
}



	function buildPackageBody(out, callback) {
		async.waterfall([
			listObjects(BUCKET, 'meta/', '.meta'),
			foreachObject(concatObjectToStream(out)),
			streamEnd(out)
		], 
		callback
		);
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

	function streamEnd(out) {
		return function(callback) {
			out.end()
			callback()
		}
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


}





var REPO_ORIGIN = 'ccare'
var REPO_LABEL = 'ccare'
var REPO_ARCH = 'amd64'
var REPO_DESCRIPTION = 'ccare repo'

function buildReleaseText(md5, size) {
	return "Origin: " + REPO_ORIGIN + "\n"
	+"Label: " + REPO_LABEL + "\n"
	+"Architectures: " + REPO_ARCH + "\n"
	+"Description: " + REPO_DESCRIPTION + "\n"
	+"MD5Sum:\n"
	+" " + md5 + "      " + size + " Packages\n"
}





module.exports.Indexer = Indexer;