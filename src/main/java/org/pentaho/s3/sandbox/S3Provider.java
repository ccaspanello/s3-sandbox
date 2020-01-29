package org.pentaho.s3.sandbox;

import com.google.common.io.ByteSource;
import com.google.common.io.Files;
import com.google.common.net.MediaType;
import lombok.extern.slf4j.Slf4j;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.options.ListContainerOptions;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;

@Slf4j
public class S3Provider implements Closeable {

  private final String name;
  private final BlobStoreContext context;

  public S3Provider( S3Config config ) {
    this.name = config.getName();
    this.context = builderContext( config );
  }

  public void createContainer( String container ) {
    log.info( "({}) Creating Container: {}", name, container );
    context.getBlobStore().createContainerInLocation( null, container );
  }

  public void deleteContainer( String container ) {
    log.info( "({}) Deleting Container: {}", name, container );
    context.getBlobStore().deleteContainer( container );
  }

  public void listContainers() {
    log.info( "({}) Listing Containers:", name );
    PageSet<? extends StorageMetadata> pageSet = context.getBlobStore().list();
    printPageSet( pageSet );
  }

  public void listContents( String container ) {
    log.info( "({}) Listing Contents @ {}:", name, container );
    PageSet<? extends StorageMetadata> pageSet = context.getBlobStore().list( container );
    printPageSet( pageSet );
  }

  public void listContents( String container, String folder ) {
    log.info( "({}) Listing Contents @ {}:", name, container + "/" + folder );
    ListContainerOptions options = ListContainerOptions.Builder.prefix( folder ).recursive();
    PageSet<? extends StorageMetadata> pageSet = context.getBlobStore().list( container, options );
    printPageSet( pageSet );
  }


  public void putBlob( String container, String fileName, File file ) {
    try {
      log.info( "({}) Adding Blob @ {}:", name, container + "/" + fileName );
      ByteSource payload = Files.asByteSource( file );
      Blob blob = context.getBlobStore().blobBuilder( fileName )
        .payload( payload )
        .contentDisposition( fileName )
        .contentLength( payload.size() )
        .contentType( MediaType.OCTET_STREAM.toString() )
        .build();
      context.getBlobStore().putBlob( container, blob );
    } catch ( IOException e ) {
      throw new S3Exception( "Unexpected error trying to put blob in container.", e );
    }
  }

  @Override
  public void close() throws IOException {
    if ( this.context != null ) {
      context.close();
    }
  }

  private BlobStoreContext builderContext( S3Config config ) {
    ContextBuilder builder = ContextBuilder.newBuilder( config.getType() );
    builder.credentials( config.getAccessKey(), config.getSecretKey() );
    // If custom endpoint is specified use it.
    if ( config.getEndpointUrl() != null ) {
      builder.endpoint( config.getEndpointUrl() );
    }
    return builder.buildView( BlobStoreContext.class );
  }

  private void printPageSet( PageSet<? extends StorageMetadata> pageSet ) {
    Iterator<? extends StorageMetadata> iterator = pageSet.iterator();
    while ( iterator.hasNext() ) {
      StorageMetadata entry = iterator.next();
      log.info( "    {}", entry.getName() );
    }
    log.info( "\n" );
  }

}
