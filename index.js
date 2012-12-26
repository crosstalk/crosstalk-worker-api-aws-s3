/*
 * index.js: Crosstalk worker wrapping AWS S3 API
 *
 * (C) 2012 Crosstalk Systems Inc.
 */
"use strict";

var async = require( 'async' ),
    crypto = require( 'crypto' ),
    https = require( 'https' ),
    inspect = require( 'inspect' ),
    logger = require( 'logger' ),
    url = require( 'url' ),
    xml2js = require( 'xml2js' );

var PARSER = new xml2js.Parser({
      normalize : false,
      trim : false,
      explicitRoot : true
    }),
    REQUEST_URI = "/",
    S3_ENDPOINT = 's3.amazonaws.com';

var attachSignatureToRequest = function attachSignatureToRequest ( dataBag, 
   callback ) {

  dataBag.requestHeaders[ 'Authorization' ] =
     dataBag.requestSignature.authorization;
  dataBag.requestHeaders[ 'Date' ] = dataBag.requestSignature.date;

  return callback( null, dataBag );

}; // attachSignatureToRequest

var deleteObject = function deleteObject ( params, callback ) {

  callback = callback || function () {}; // req-reply pattern is optional

  //
  // required params
  //
  var awsAccessKeyId = params.awsAccessKeyId,
      bucketName = params.bucketName,
      objectName = params.objectName,
      secretAccessKey = params.secretAccessKey;

  if ( ! awsAccessKeyId ) return callback( { message : "missing awsAccessKeyId" } );
  if ( ! bucketName ) return callback( { message : "missing bucketName" } );
  if ( ! objectName ) return callback( { message : "missing objectName" } );
  if ( ! secretAccessKey ) return callback( { message : "missing secretAccessKey" } );

  var requestHeaders = {};

  //
  // optional params
  //
  if ( params.mfa ) requestHeaders[ 'x-amz-mfa' ] = params.mfa;

  return executeAction({
    awsAccessKeyId : awsAccessKeyId,
    bucketName : bucketName,
    httpVerb : "DELETE",
    objectName : objectName,
    requestHeaders : requestHeaders,
    secretAccessKey : secretAccessKey
  }, callback );

}; // deleteObject

var executeAction = function executeAction ( dataBag, callback ) {

  async.waterfall([

    // bootstrap dataBag
    function ( _callback ) {
      return _callback( null, dataBag );
    },

    getRequestSignature,

    attachSignatureToRequest,

    makeRequest,

    parseResponse

  ], function ( error, result ) {

    if ( error ) return callback( error );

    return callback( null, result );

  }); // async.waterfall

}; // executeAction

var extractXAmzExpiration = function extractXAmzExpiration ( xAmzExpiration ) {

  if ( ! xAmzExpiration ) return; // nothing to do

  var expiration;

  // x-amz-expiration: expiry-date="Fri, 23 Dec 2012 00:00:00 GMT", rule-id="1"
  
  // extract expiry-date
  var expiryDate = xAmzExpiration.match( /expiry-date="(.*)",/ );

  if ( expiryDate ) {
  
    expiryDate = expiryDate[ 1 ];
    expiryDate = new Date( expiryDate ).toISOString();

  } // if ( expiryDate )

  // extract rule-id
  var ruleId = xAmzExpiration.match( /rule-id="(.*)"/ );

  ruleId ? ruleId = ruleId[ 1 ] : null;

  if ( expiryDate && ruleId ) {
    
    expiration = {
      'expiry-date' : expiryDate,
      'rule-id' : ruleId
    }

  } // if ( expiryDate && ruleId )

  return expiration;

}; // extractXAmzExpiration

var getBucket = function getBucket ( params, callback ) {

  if ( ! callback ) return; // nothing to do

  //
  // required params
  //
  var awsAccessKeyId = params.awsAccessKeyId,
      bucketName = params.bucketName,
      secretAccessKey = params.secretAccessKey;

  if ( ! awsAccessKeyId ) return callback( { message : "missing awsAccessKeyId" } );
  if ( ! bucketName ) return callback( { message : "missing bucketName" } );
  if ( ! secretAccessKey ) return callback( { message : "missing secretAccessKey" } );

  var requestHeaders = {},
      requestParams = [];

  //
  // optional params
  //
  if ( params.delimiter ) {
    requestParams.push( "delimiter=" + encodeURIComponent( params.delimiter ) );
  }
  if ( params.marker ) {
    requestParams.push( "marker=" + encodeURIComponent( params.marker ) );
  }
  if ( params.maxKeys ) {
    requestParams.push( "max-keys=" + encodeURIComponent( params.maxKeys ) );
  }
  if ( params.prefix ) {
    requestParams.push( "prefix=" + encodeURIComponent( params.prefix ) );
  }

  return executeAction({
    actionType : "bucket",
    awsAccessKeyId : awsAccessKeyId,
    bucketName : bucketName,
    httpVerb : "GET",
    requestHeaders : requestHeaders,
    requestParams : requestParams,
    secretAccessKey : secretAccessKey
  }, callback );

}; // getBucket

var getObject = function getObject ( params, callback ) {

  if ( ! callback ) return; // nothing to do

  //
  // required params
  //
  var awsAccessKeyId = params.awsAccessKeyId,
      bucketName = params.bucketName,
      objectName = params.objectName,
      secretAccessKey = params.secretAccessKey;

  if ( ! awsAccessKeyId ) return callback( { message : "missing awsAccessKeyId" } );
  if ( ! bucketName ) return callback( { message : "missing bucketName" } );
  if ( ! objectName ) return callback( { message : "missing objectName" } );
  if ( ! secretAccessKey ) return callback( { message : "missing secretAccessKey" } );

  var requestHeaders = {};

  //
  // optional params
  //
  if ( params.ifMatch ) requestHeaders[ 'If-Match' ] = params.ifMatch;
  if ( params.ifModifiedSince ) {
    requestHeaders[ 'If-Modified-Since' ] = params.ifModifiedSince;
  }
  if ( params.ifNoneMatch ) {
    requestHeaders[ 'If-None-Match' ] = params.ifNoneMatch;
  }
  if ( params.ifUnmodifiedSince ) {
    requestHeaders[ 'If-Unmodified-Since' ] = params.ifUnmodifiedSince;
  }
  if ( params.range ) requestHeaders[ 'Range' ] = params.range;
  if ( params.responseCacheControl ) {
    requestHeaders[ 'response-cache-control' ] = params.responseCacheControl;
  }
  if ( params.responseContentDisposition ) {
    
    requestHeaders[ 'response-content-disposition' ] = 
       params.responseContentDisposition;

  } // if ( params.responseContentDisposition )
  if ( params.responseContentEncoding ) {
    
    requestHeaders[ 'response-content-encoding' ] = 
       params.responseContentEncoding;

  } // if ( params.responseContentEncoding )
  if ( params.responseContentLanguage ) {

    requestHeaders[ 'response-content-language' ] = 
       params.responseContentLanguage;

  } // if ( params.responseContentLanguage )
  if ( params.responseContentType ) {
    requestHeaders[ 'response-content-type' ] = params.responseContentType;
  }
  if ( params.responseExpires ) {
    requestHeaders[ 'response-expires' ] = params.responseExpires;
  }

  return executeAction({
    awsAccessKeyId : awsAccessKeyId,
    bucketName : bucketName,
    httpVerb : "GET",
    objectName : objectName,
    requestHeaders : requestHeaders,
    secretAccessKey : secretAccessKey
  }, callback );

}; // getObject

var getRequestSignature = function getRequestSignature ( dataBag, callback ) {

  crosstalk.emit( '~crosstalk.api.aws.signature.s3', {
    awsAccessKeyId : dataBag.awsAccessKeyId,
    bucketName : dataBag.bucketName,
    headers : dataBag.requestHeaders,
    httpVerb : dataBag.httpVerb,
    objectName : dataBag.objectName || "",
    secretAccessKey : dataBag.secretAccessKey
  }, '~crosstalk', function ( error, response ) {

    if ( error ) return callback( error );

    dataBag.requestSignature = response;
    return callback( null, dataBag );

  }); // crosstalk.emit ~crosstalk.api.aws.signature.s3

}; // getRequestSignature

var makeRequest = function makeRequest ( dataBag, callback ) {

  var requestOptions = {
    headers : dataBag.requestHeaders,
    host : dataBag.bucketName + "." + S3_ENDPOINT,
    method : dataBag.httpVerb,
    path : "/" 
  };

  if ( dataBag.objectName ) requestOptions.path += dataBag.objectName;

  if ( dataBag.requestParams && dataBag.requestParams.length > 0 ) {
    requestOptions.path += "?" + dataBag.requestParams.join( "&" );
  }

  var req = https.request( requestOptions );

  req.on( 'response', function ( response ) {

    var responseBody = "";

    response.setEncoding( "utf8" );
    response.on( "data", function ( chunk ) {
      responseBody += chunk;
    });

    response.on( "end", function () {

      dataBag.responseBody = responseBody;
      dataBag.responseHeaders = response.headers;
      dataBag.statusCode = response.statusCode;

      return callback( null, dataBag );

    }); // response.on "end"

  }); // req.on 'response'

  req.on( 'error', function ( error ) {
    return callback( error );
  });

  logger.log( 'request body', dataBag.body );
  if ( dataBag.body ) req.write( dataBag.body );

  req.end();

}; // makeRequest

var parseDeleteResponse = function parseDeleteResponse ( dataBag, callback ) {

  if ( dataBag.statusCode == 204 ) {

    var result = {};

    if ( dataBag.responseHeaders[ 'x-amz-delete-marker' ] ) {
      result.deleteMarker = true;
    }
    if ( dataBag.responseHeaders[ 'x-amz-request-id' ] ) {
      result.requestId = dataBag.responseHeaders[ 'x-amz-request-id' ];
    }    
    if ( dataBag.responseHeaders[ 'x-amz-version-id' ] ) {
      result.versionId = dataBag.responseHeaders[ 'x-amz-version-id' ];
    }    

    return callback( null, result );

  } // if ( dataBag.statusCode == 204 )

  logger.debug( dataBag.responseBody );

  return parseErrorResponse( dataBag.responseBody, callback );

}; // parseDeleteResponse

var parseErrorResponse = function parseErrorResponse ( responseBody, callback ) {

  PARSER.parseString( responseBody, function ( error, result ) {

    if ( error ) return callback( { message : responseBody } );

    return callback( {
      argumentName : result.Error.ArgumentName && 
         result.Error.ArgumentName[ 0 ],
      argumentValue : result.Error.ArgumentValue && 
         result.Error.ArgumentValue[ 0 ],
      awsAccessKeyId : result.Error.AWSAccessKeyId &&
         result.Error.AWSAccessKeyId[ 0 ],
      code : result.Error.Code && result.Error.Code[ 0 ],
      hostId : result.Error.HostId && result.Error.HostId[ 0 ],
      message : result.Error.Message && result.Error.Message[ 0 ],
      resource : result.Error.Resource && result.Error.Resource[ 0 ],
      requestId : result.Error.RequestId && result.Error.RequestId[ 0 ],
      signatureProvided : result.Error.SignatureProvided &&
         result.Error.SignatureProvided[ 0 ],
      stringToSignBytes : result.Error.StringToSignBytes &&
         result.Error.StringToSignBytes[ 0 ],
      stringToSign : result.Error.StringToSign && result.Error.StringToSign[ 0 ]
    });

  }); // PARSER.parseString

}; // parseErrorResponse

var parseGetBucketResponse = function parseGetBucketResponse ( dataBag, callback ) {

  PARSER.parseString( dataBag.responseBody, function ( error, parsed ) {

    if ( error ) return callback( { message : dataBag.responseBody } );

    var result = {};

    logger.log( inspect( parsed, false, null ) );

    var bucketList = parsed.ListBucketResult;

    if ( ! bucketList ) return callback( { message : parsed } );

    result.isTruncated = ( bucketList.IsTruncated && 
       bucketList.IsTruncated[ 0 ].toLowerCase() == 'true' ) || false;

    if ( bucketList.Marker && typeof( bucketList.Marker[ 0 ] ) === 'string' ) {
      result.marker = bucketList.Marker[ 0 ];
    }
    result.maxKeys = bucketList.MaxKeys && bucketList.MaxKeys[ 0 ];
    result.name = bucketList.Name && bucketList.Name[ 0 ];
    if ( bucketList.Prefix && typeof( bucketList.Prefix[ 0 ] ) === 'string' ) {
      result.prefix = bucketList.Prefix[ 0 ];
    }

    result.contents = [];

    if ( bucketList.Contents ) {

      bucketList.Contents.forEach( function ( bucketItem ) {

        var item = {};

        item.ETag = bucketItem.ETag && bucketItem.ETag[ 0 ].replace( /"/g, "" );
        item.key = bucketItem.Key && bucketItem.Key[ 0 ];
        item.lastModified = bucketItem.LastModified &&
           bucketItem.LastModified[ 0 ];
        item.size = bucketItem.Size && bucketItem.Size[ 0 ];
        item.storageClass = bucketItem.StorageClass && 
           bucketItem.StorageClass[ 0 ];

        if ( bucketItem.Owner ) {

          var owner = {};
          
          owner.displayName = bucketItem.Owner[ 0 ].DisplayName &&
             bucketItem.Owner[ 0 ].DisplayName[ 0 ];
          owner.id = bucketItem.Owner[ 0 ].ID &&
             bucketItem.Owner[ 0 ].ID[ 0 ];

          item.owner = owner;

        } // if ( bucketItem.Owner )

        result.contents.push( item );

      }); // bucketList.Contents.forEach

    } // if ( bucketList.Contents )

    return callback( null, result );

  }); // PARSER.parseString

}; // parseGetBucketResponse

var parseGetResponse = function parseGetResponse ( dataBag, callback ) {

  if ( dataBag.statusCode == 200 ) {

    var result = {};

    if ( dataBag.responseBody && dataBag.actionType === "bucket" ) {
      return parseGetBucketResponse( dataBag, callback );
    } else if ( dataBag.responseBody ) {
      result.object = dataBag.responseBody;
    } // if ( dataBag.responseBody )
    if ( dataBag.responseHeaders[ 'etag' ] ) {
      result.ETag = dataBag.responseHeaders[ 'etag' ].replace( /"/g, "" );
    }
    if ( dataBag.responseHeaders[ 'x-amz-delete-marker' ] ) {
      result.deleteMarker = true;
    }
    if ( dataBag.responseHeaders[ 'x-amz-expiration' ] ) {

      result.expiration =
         extractXAmzExpiration( dataBag.responseHeaders[ 'x-amz-expiration' ] );

    } // if ( dataBag.responseHeaders[ 'x-amz-expiration' ] )
    if ( dataBag.responseHeaders[ 'x-amz-request-id' ] ) {
      result.requestId = dataBag.responseHeaders[ 'x-amz-request-id' ];
    }
    if ( dataBag.responseHeaders[ 'x-amz-restore' ] ) {
      result.restore = dataBag.responseHeaders[ 'x-amz-restore' ];
    }
    if ( dataBag.responseHeaders[ 'x-amz-server-side-encryption' ] ) {

      result.serverSideEncryption =
         dataBag.responseHeaders[ 'x-amz-server-side-encryption' ];

    } // if ( dataBag.responseHeaders[ 'x-amz-server-side-encryption' ] )
    if ( dataBag.responseHeaders[ 'x-amz-version-id' ] ) {
      result.versionId = dataBag.responseHeaders[ 'x-amz-version-id' ];
    }
    if ( dataBag.responseHeaders[ 'x-amz-website-redirect-location' ] ) {

      result.websiteRedirectLocation = 
         dataBag.responseHeaders[ 'x-amz-website-redirect-location' ];

    } // if ( dataBag.responseHeaders[ 'x-amz-website-redirect-location' ] )

    return callback( null, result );

  } // if ( dataBag.statusCode == 200 )

  return parseErrorResponse( dataBag.responseBody, callback );

}; // parseGetResponse

var parsePutResponse = function parsePutResponse ( dataBag, callback ) {

  // we don't need data (that contains the error) if we succeeded
  if ( dataBag.statusCode == 200 ) {

    var result = {};
    if ( dataBag.responseHeaders[ 'etag' ] ) {
      result.ETag = dataBag.responseHeaders[ 'etag' ].replace( /"/g, "" );
    }
    if ( dataBag.responseHeaders[ 'x-amz-expiration' ] ) {
      
      result.expiration = 
         extractXAmzExpiration( dataBag.responseHeaders[ 'x-amz-expiration' ] );

    } // if ( dataBag.responseHeaders[ 'x-amz-expiration' ] )
    if ( dataBag.responseHeaders[ 'x-amz-server-side-encryption' ] ) {

      result.serverSideEncryption =
         dataBag.responseHeaders[ 'x-amz-server-side-encryption' ];

    } // if ( dataBag.responseHeaders[ 'x-amz-server-side-encryption' ] )
    if ( dataBag.responseHeaders[ 'x-amz-version-id' ] ) {
      result.versionId = dataBag.responseHeaders[ 'x-amz-version-id' ];
    }
    if ( dataBag.responseHeaders[ 'x-amz-request-id' ] ) {
      result.requestId = dataBag.responseHeaders[ 'x-amz-request-id' ];
    }

    return callback( null, result );

  } // if ( dataBag.statusCode == 200 )

  return parseErrorResponse( dataBag.responseBody, callback );

}; // parsePutResponse

var parseResponse = function parseResponse ( dataBag, callback ) {

  logger.debug( inspect( dataBag.responseHeaders, false, null ) );
  logger.debug( dataBag.responseBody );

  if ( dataBag.httpVerb.toLowerCase() === "put" ) {
    return parsePutResponse( dataBag, callback );
  } else if ( dataBag.httpVerb.toLowerCase() === "get" ) {
    return parseGetResponse( dataBag, callback );
  } else if ( dataBag.httpVerb.toLowerCase() === "delete" ) {
    return parseDeleteResponse( dataBag, callback );
  }

  return callback( { 
    message : "Not Implemented", 
    response : dataBag.responseBody 
  });

}; // parseResponse

var putObject = function putObject ( params, callback ) {

  callback = callback || function () {}; // req-reply pattern is optional

  //
  // required params
  //
  var awsAccessKeyId = params.awsAccessKeyId,
      bucketName = params.bucketName,
      object = params.object,
      objectName = params.objectName,
      secretAccessKey = params.secretAccessKey;

  if ( ! awsAccessKeyId ) return callback( { message : "missing awsAccessKeyId" } );
  if ( ! bucketName ) return callback( { message : "missing bucketName" } );
  if ( ! object && object !== "" ) return callback( { message : "missing object" } );
  if ( ! objectName ) return callback( { message : "missing objectName" } );
  if ( ! secretAccessKey ) return callback( { message : "missing secretAccessKey" } );

  // as a convenience, if object is a JavaScript object, convert it to JSON
  if ( typeof( object ) === 'object' ) {

    try {
      object = JSON.stringify( object );
    } catch ( exception ) {
    
      return callback( { 
        errorCode : 400, 
        message : "Could not convert object to JSON" 
      });

    } // catch ( exception )

  } // if ( typeof( object ) === 'object' )

  var requestHeaders = {};

  //
  // optional params
  //
  if ( params.acl ) requestHeaders[ 'x-amz-acl' ] = params.acl;
  if ( params.cacheControl ) {
    requestHeaders[ 'Cache-Control' ] = params.cacheControl;
  }
  if ( params.contentDisposition ) {
    requestHeaders[ 'Content-Disposition' ] = params.contentDisposition;
  }
  if ( params.contentEncoding ) {
    requestHeaders[ 'Content-Encoding' ] = params.contentEncoding;
  }
  if ( params.contentLength ) {
    requestHeaders[ 'Content-Length' ] = params.contentLength;
  }
  if ( params.contentMD5 ) {
    requestHeaders[ 'Content-MD5' ] = params.contentMD5;
  }
  if ( params.contentType ) {
    requestHeaders[ 'Content-Type' ] = params.contentType;
  }
  if ( params.expires ) requestHeaders[ 'Expires' ] = params.expires;
  if ( params.grantRead ) {
    requestHeaders[ 'x-amz-grant-read' ] = params.grantRead;
  }
  if ( params.grantReadAcp ) {
    requestHeaders[ 'x-amz-grant-read-acp' ] = params.grantReadAcp;
  }
  if ( params.grantWriteAcp ) {
    requestHeaders[ 'x-amz-grant-write-acp' ] = params.grantWriteAcp;
  }
  if ( params.grantFullControl ) {
    requestHeaders[ 'x-amz-grant-full-control' ] = params.grantFullControl;
  }
  if ( params.meta ) {

    Object.keys( meta ).forEach( function ( header ) {
      requestHeaders[ 'x-amz-meta-' + header ] = params.meta[ header ];
    });

  } // if ( params.meta )
  if ( params.serverSideEncryption ) {

    requestHeaders[ 'x-amz-server-side-encryption' ] = 
       params.serverSideEncryption;

  } // if ( params.serverSideEncryption )
  if ( params.storageClass ) {
    requestHeaders[ 'x-amz-storage-class' ] = params.storageClass;
  }
  if ( params.websiteRedirectLocation ) {

    requestHeaders[ 'x-amz-website-redirect-location' ] =
       params.websiteRedirectLocation;

  } // if ( params.websiteRedirectLocation )

  return executeAction( {
    awsAccessKeyId : awsAccessKeyId,
    body : object,
    bucketName : bucketName,
    httpVerb : "PUT",
    objectName : objectName,
    requestHeaders : requestHeaders,
    secretAccessKey : secretAccessKey
  }, callback );

}; // putObject

crosstalk.on( 'api.aws.s3.deleteObject@v1', 'public', deleteObject );
crosstalk.on( 'api.aws.s3.getBucket@v1', 'public', getBucket );
crosstalk.on( 'api.aws.s3.getObject@v1', 'public', getObject );
crosstalk.on( 'api.aws.s3.putObject@v1', 'public', putObject );