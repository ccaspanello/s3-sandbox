package org.pentaho.s3.sandbox;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class S3Config {

  private final String name;
  private final String type;
  private final String accessKey;
  private final String secretKey;
  private final String endpointUrl;

}
