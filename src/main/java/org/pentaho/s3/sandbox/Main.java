package org.pentaho.s3.sandbox;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;

@Slf4j
public class Main {

  public static void main( String[] args ) {

    // HCP Test
    S3Config hcpConfig = S3Config.builder()
      .name( "HCP ccaspanello" )
      .type( "s3" )
      .accessKey( "_____" )
      .secretKey( "_____" )
      .endpointUrl( "_____" )
      .build();
    runTest( hcpConfig );

    // MinIO Test
    S3Config minioConfig = S3Config.builder()
      .name( "MinIO S3" )
      .type( "s3" )
      .accessKey( "_____" )
      .secretKey( "_____" )
      .endpointUrl( "_____" )
      .build();
    runTest( minioConfig );

    // AWS Test
    S3Config awsConfig = S3Config.builder()
      .name( "AWS S3" )
      .type( "aws-s3" )
      .accessKey( "_____" )
      .secretKey( "_____" )
      .build();
    runTest( awsConfig );
  }

  private static void runTest( S3Config config ) {
    try ( S3Provider provider = new S3Provider( config ) ) {
      String containerName = "ccaspanello-movies";

      File file1 = new File( Main.class.getResource( "/test1.txt" ).getFile() );
      File file2 = new File( Main.class.getResource( "/test2.txt" ).getFile() );
      provider.listContainers();
      provider.createContainer( containerName );
      provider.listContainers();

      provider.putBlob( containerName, "data/test1.txt", file1 );
      provider.putBlob( containerName, "data/test2.txt", file2 );
      provider.putBlob( containerName, "otherData/test1.txt", file1 );
      provider.putBlob( containerName, "otherData/test2.txt", file2 );

      provider.listContents( containerName );
      provider.listContents( containerName, "data" );
      provider.listContents( containerName, "otherData" );

      provider.deleteContainer( containerName );
      provider.listContainers();
    } catch ( IOException e ) {
      throw new S3Exception( "Unexpected error running test", e );
    }
  }
}
