package org.pentaho.s3.sandbox;

public class S3Exception extends RuntimeException {

  public S3Exception( String msg ) {
    super( msg );
  }

  public S3Exception( String msg, Throwable throwable ) {
    super( msg, throwable );
  }

}
