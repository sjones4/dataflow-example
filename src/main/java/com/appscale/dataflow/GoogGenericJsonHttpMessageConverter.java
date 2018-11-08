/*
 * Copyright 2018 AppScale Systems, Inc
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.appscale.dataflow;

import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.MediaType;
import org.springframework.http.converter.AbstractGenericHttpMessageConverter;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.http.converter.HttpMessageNotWritableException;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import com.google.api.client.json.GenericJson;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.JsonGenerator;
import com.google.api.client.json.JsonParser;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.common.collect.Lists;

/**
 * HttpMessageConverter supporting the Google clients GenericJson messages
 */
public class GoogGenericJsonHttpMessageConverter extends AbstractGenericHttpMessageConverter<Object> {

  private final JsonFactory factory = JacksonFactory.getDefaultInstance( );

  public GoogGenericJsonHttpMessageConverter( ) {
    super( );
    setDefaultCharset( StandardCharsets.UTF_8 );
    setSupportedMediaTypes( Lists.newArrayList( MediaType.APPLICATION_JSON_UTF8, MediaType.APPLICATION_JSON ) );
  }

  @Override
  protected boolean supports( final Class<?> clazz ) {
    return GenericJson.class.isAssignableFrom( clazz );
  }

  @NonNull
  @Override
  public Object read(
      @NonNull final Type type,
      @Nullable final Class<?> contextClass,
      @NonNull final HttpInputMessage inputMessage
  ) throws IOException, HttpMessageNotReadableException {
    final JsonParser parser = factory.createJsonParser( inputMessage.getBody( ) );
    try {
      return parser.parse( type, false );
    } finally {
      parser.close( );
    }
  }

  @NonNull
  @Override
  protected Object readInternal(
      @NonNull final Class<?> clazz,
      @NonNull final HttpInputMessage inputMessage
  ) throws IOException, HttpMessageNotReadableException {
    final JsonParser parser = factory.createJsonParser( inputMessage.getBody( ) );
    try {
      return parser.parse( clazz, false );
    } finally {
      parser.close( );
    }
  }

  @NonNull
  @Override
  protected void writeInternal(
      @NonNull final Object object,
      @Nullable final Type type,
      @NonNull final HttpOutputMessage outputMessage
  ) throws IOException, HttpMessageNotWritableException {
    final JsonGenerator generator = factory.createJsonGenerator( outputMessage.getBody( ), StandardCharsets.UTF_8 );
    try {
      generator.serialize( object );
    } finally {
      generator.close( );
    }
  }
}
