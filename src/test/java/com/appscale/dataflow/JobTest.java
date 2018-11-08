/*
 * Copyright 2018 AppScale Systems, Inc
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.appscale.dataflow;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Map;
import org.junit.Test;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.DataflowRequest;
import com.google.api.services.dataflow.DataflowRequestInitializer;
import com.google.api.services.dataflow.model.CreateJobFromTemplateRequest;
import com.google.api.services.dataflow.model.GetTemplateResponse;
import com.google.api.services.dataflow.model.Job;
import com.google.api.services.dataflow.model.ListJobsResponse;
import com.google.api.services.dataflow.model.RuntimeEnvironment;
import com.google.api.services.dataflow.model.Step;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 *
 */
public class JobTest {

  @Test
  public void testCreateJob( ) throws Exception {
    final Dataflow dataflow = dataflow( "http://localhost:8080/" );
    final Map<String,String> labels = Maps.newHashMap();
    labels.put( "label1", "value1" );
    labels.put( "label2", "value2" );
    final Map<String,Object> stepProperties = Maps.newHashMap();
    stepProperties.put( "prop1", "value1" );
    stepProperties.put( "prop2", "value2" );
    stepProperties.put( "prop3", "value3" );
    final Job result = dataflow.projects().jobs()
        .create( "project-1",
            new Job( )
                .setName( "job-1" )
                .setType( "JOB_TYPE_BATCH" )
                .setLabels( labels )
                .setSteps( Lists.newArrayList(
                    new Step( )
                      .setKind( "kind" )
                      .setName( "step-1" )
                      .setProperties( stepProperties )
                ) )
        )
        .execute( );
    System.out.println( result );
  }

  @Test
  public void testListJobs( ) throws Exception {
    final Dataflow dataflow = dataflow( "http://localhost:8080/" );
    final ListJobsResponse result = dataflow.projects().jobs()
        .list( "project-1" )
        .execute( );
    System.out.println( result );
  }

  @Test
  public void testCreateTemplate( ) throws Exception {
    final Dataflow dataflow = dataflow( "http://localhost:8080/" );
    final Map<String,String> params = Maps.newHashMap( );
    params.put( "a", "b" );
    final Job result = dataflow.projects().templates()
        .create( "project-1",
            new CreateJobFromTemplateRequest()
                .setGcsPath( "gs://path/here" )
                .setJobName( "job-5" )
                .setLocation( "foo" )
                .setParameters( params )
                .setEnvironment(
                    new RuntimeEnvironment( ).setMaxWorkers( 10 )
                )
        )
        .execute( );
    System.out.println( result );
  }

  @Test
  public void testGetTemplate( ) throws Exception {
    final Dataflow dataflow = dataflow( "http://localhost:8080/" );
    final GetTemplateResponse result = dataflow.projects().templates()
        .get( "project-1" ).setGcsPath( "gs://path/here" )
        .execute( );
    System.out.println( result );
  }

  private Dataflow dataflow( final String rootUrl ) throws GeneralSecurityException, IOException {
    return
        new Dataflow.Builder( GoogleNetHttpTransport.newTrustedTransport( ), new JacksonFactory( ), null )
            .setApplicationName( "test" )
            .setDataflowRequestInitializer( new GzipDisableInitializer( ) )
            .setRootUrl( rootUrl )
            .build( );
  }

  private static final class GzipDisableInitializer extends DataflowRequestInitializer {
    @Override
    protected void initializeDataflowRequest( final DataflowRequest<?> request ) throws IOException {
      super.initializeDataflowRequest( request );
      // spring web does not support request compression (with tomcat)
      // could use jetty or undertow with appropriate config such as
      // io.undertow.server.handlers.encoding.RequestEncodingHandler
      request.setDisableGZipContent( true );
    }
  }
}
