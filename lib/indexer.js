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

function Indexer(knoxClient) {

	var s3Client = s3Lib.fromKnox(knoxClient);

	this.reIndex = function(callback) {
		async.waterfall([
			listDebsAndMetas,
			findMissingMetadata,
			indexItems,
			deleteTempDirs
		], callback );
	}

	function listDebsAndMetas(callback) {
		async.parallel({
		    debArchives: listObjects('deb/', '.deb'),
		    metaFiles: listObjects('meta/', '.meta')
		}, callback);
	}

	function findMissingMetadata(currentFiles, callback) {
		var debArchives = currentFiles.debArchives
		var metaFiles = currentFiles.metaFiles
		var missing = []
		debArchives.forEach(function(deb, idx) {
			var expectedMeta = deb + ".meta";
			if (metaFiles.indexOf(expectedMeta) < 0) {
				missing.push( { 'deb': deb, 'meta': expectedMeta } )
			}
		})
		callback(null, missing)
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


	var BUCKET = 'debby'



	function listObjects(prefix, extension) {
		return function(callback) {
			s3.client.listObjects( 
				{ 'Bucket': BUCKET, 'Prefix': prefix},
				function(err, response) {
					var objectNames = toNameList(response.Contents, prefix, extension)
					callback(err, objectNames)
				}
			)
		}
	} 

}



module.exports.Indexer = Indexer;