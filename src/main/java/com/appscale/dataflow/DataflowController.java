/*
 * Copyright 2018 AppScale Systems, Inc
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.appscale.dataflow;

import static org.springframework.http.MediaType.APPLICATION_JSON_UTF8_VALUE;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import com.google.api.services.dataflow.model.CreateJobFromTemplateRequest;
import com.google.api.services.dataflow.model.GetTemplateResponse;
import com.google.api.services.dataflow.model.Job;
import com.google.api.services.dataflow.model.ListJobsResponse;
import com.google.api.services.dataflow.model.Status;
import com.google.common.collect.Lists;

/**
 * Controller exposing Google Dataflow service methods
 */
@RestController
public class DataflowController {
  @PostMapping(path="/v1b3/projects/{projectId}/jobs", consumes=APPLICATION_JSON_UTF8_VALUE, produces=APPLICATION_JSON_UTF8_VALUE)
  Job createJob(
      @PathVariable final String projectId,
      @RequestParam(required = false) final String location,
      @RequestParam(defaultValue="JOB_VIEW_SUMMARY") final String view,
      @RequestBody final Job job
  ) {
    job.setProjectId( projectId );
    return job;
  }

  @GetMapping(path="/v1b3/projects/{projectId}/jobs", produces=APPLICATION_JSON_UTF8_VALUE)
  ListJobsResponse listJobs(
      @PathVariable final String projectId,
      @RequestParam(defaultValue="UNKNOWN") final String filter,
      @RequestParam(defaultValue="JOB_VIEW_SUMMARY") final String view
  ) {
    return new ListJobsResponse().setNextPageToken( "page-2" ).setJobs( Lists.newArrayList(
        new Job()
            .setProjectId( projectId )
            .setId( "job-1" )
    ) );
  }

  @PostMapping(path="/v1b3/projects/{projectId}/templates", consumes=APPLICATION_JSON_UTF8_VALUE, produces=APPLICATION_JSON_UTF8_VALUE)
  Job createTemplate(
      @PathVariable final String projectId,
      @RequestBody final CreateJobFromTemplateRequest createJobFromTemplateRequest
  ) {
    return new Job()
        .setProjectId( projectId )
        .setId( "job-1" );
  }

  @GetMapping(path="/v1b3/projects/{projectId}/templates:get", produces=APPLICATION_JSON_UTF8_VALUE)
  GetTemplateResponse getTemplate(
      @PathVariable final String projectId,
      @RequestParam(required = false) final String location,
      @RequestParam(defaultValue="JOB_VIEW_SUMMARY") final String view,
      @RequestParam final String gcsPath
  ) {
    return new GetTemplateResponse( )
        .setStatus( new Status().setCode( 404 ).setMessage( "Not Found" ) );
  }
}
