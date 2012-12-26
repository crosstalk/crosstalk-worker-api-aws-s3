/*
 * integration.test.js
 *
 * (C) 2012 Crosstalk Systems Inc.
 */
"use strict";

var CONFIG = require( './private.config.json' );

var ide = require( 'crosstalk-ide' )(),
    workerPath = require.resolve( '../index' );

var worker;

worker = ide.run( workerPath, {} );

var content = JSON.stringify( { my : "test object" } );
var contentLength = Buffer.byteLength( content, 'utf8' );

var validPutObject = {
  acl : "private",
  awsAccessKeyId : CONFIG.awsAccessKeyId,
  bucketName : CONFIG.bucketName,
  contentLength : contentLength,
  contentType : 'application/json',
  object : content,
  objectName : "2012-12-26/23/myObject.json",
  secretAccessKey : CONFIG.secretAccessKey,
  serverSideEncryption : "AES256"
};

worker.dontMockHttps = true;

worker.proxy = "~crosstalk.api.aws.signature.s3";
worker.crosstalkToken = CONFIG.crosstalkToken;

worker.send( "api.aws.s3.putObject@v1", validPutObject, 'public', true );

var validGetObject = {
  awsAccessKeyId : CONFIG.awsAccessKeyId,
  bucketName : CONFIG.bucketName,
  objectName : "myObject.json",
  secretAccessKey : CONFIG.secretAccessKey
};

worker.send( "api.aws.s3.getObject@v1", validGetObject, 'public', true );

var validDeleteObject = {
  awsAccessKeyId : CONFIG.awsAccessKeyId,
  bucketName : CONFIG.bucketName,
  objectName : "myObject.json",
  secretAccessKey : CONFIG.secretAccessKey
};

//worker.send( "api.aws.s3.deleteObject@v1", validDeleteObject, 'public', true );

var validGetBucket = {
  awsAccessKeyId : CONFIG.awsAccessKeyId,
  bucketName : CONFIG.bucketName,
  prefix : "2012-12-26/23",
  secretAccessKey : CONFIG.secretAccessKey
};

worker.send( "api.aws.s3.getBucket@v1", validGetBucket, 'public', true );