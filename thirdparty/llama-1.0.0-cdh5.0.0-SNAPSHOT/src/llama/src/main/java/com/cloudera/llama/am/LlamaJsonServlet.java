/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.llama.am;

import com.cloudera.llama.util.UUID;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class LlamaJsonServlet extends HttpServlet {
  private static final Logger LOG =
      LoggerFactory.getLogger(LlamaJsonServlet.class);

  public static final String REST_DATA = "llama.rest.data";

  public static final String PATH = "/json";
  public static final String BIND_PATH = PATH + "/*";

  public static final String V1 = "/v1";

  public static final String V1_PATH =  PATH + "/v1";

  public static final String ROOT = "/";
  public static final String SUMMARY = V1 + "/summary";
  public static final String ALL = V1 + "/all";
  public static final String RESERVATION = V1 + "/reservation/";
  public static final String HANDLE = V1 + "/handle/";
  public static final String NODE = V1 + "/node/";
  public static final String QUEUE = V1 + "/queue/";

  private static final Map REST_API = new LinkedHashMap();

  static {
    Map urls = new LinkedHashMap();
    urls.put("summary", SUMMARY);
    urls.put("all", ALL);
    urls.put("queue", QUEUE + "<?>");
    urls.put("handle", HANDLE + "<?>");
    urls.put("node", NODE + "<?>");
    urls.put("reservation", RESERVATION + "<?>");
    REST_API.put(RestData.REST_VERSION_KEY, RestData.REST_VERSION_VALUE);
    REST_API.put("urls", urls);
  }

  private static final String APPLICATION_JSON_MIME = "application/json";
  private static final String TEXT_PLAIN_MIME = "text/plain";

  private RestData restData;
  private ObjectWriter jsonWriter;

  @Override
  public void init() throws ServletException {
    restData = (RestData) getServletContext().getAttribute(REST_DATA);
    if (restData == null) {
      throw new RuntimeException("RestData not available in the ServletContext");
    }
    ObjectMapper mapper = new ObjectMapper();
    jsonWriter = mapper.defaultPrettyPrintingWriter();
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    resp.setContentType(APPLICATION_JSON_MIME);
    resp.setStatus(HttpServletResponse.SC_OK);
    String requestType = req.getPathInfo();
    requestType =  (requestType != null) ? requestType.toLowerCase() : ROOT;
    try {
      if (requestType.equals(ROOT)) {
        resp.sendRedirect(V1_PATH);
      } else if (requestType.equals(V1) || requestType.equals(V1 + "/")) {
        jsonWriter.writeValue(resp.getWriter(), REST_API);
      } else if (requestType.equals(SUMMARY)) {
        restData.writeSummaryAsJson(resp.getWriter());
      } else if (requestType.equals(ALL)) {
        restData.writeAllAsJson(resp.getWriter());
      } else if (requestType.startsWith(RESERVATION)) {
        UUID id = UUID.fromString(requestType.substring(RESERVATION.length()));
        restData.writeReservationAsJson(id, resp.getWriter());
      } else if (requestType.startsWith(HANDLE)) {
        UUID id = UUID.fromString(requestType.substring(HANDLE.length()));
        restData.writeHandleReservationsAsJson(id, resp.getWriter());
      } else if (requestType.startsWith(NODE)) {
        String node = requestType.substring(NODE.length());
        restData.writeNodeResourcesAsJson(node, resp.getWriter());
      } else if (requestType.startsWith(QUEUE)) {
        String queue = requestType.substring(QUEUE.length());
        restData.writeQueueReservationsAsJson(queue, resp.getWriter());
      } else {
        throw new Exception();
      }
    } catch (RestData.NotFoundException ex) {
      resp.setContentType(TEXT_PLAIN_MIME);
      resp.setStatus(HttpServletResponse.SC_NOT_FOUND);
      resp.getWriter().println("NOT FOUND");
    } catch (Throwable ex) {
      LOG.warn("Error while processing '{}', {}", getFullUrl(req),
          ex.toString(), ex);
      resp.setContentType(TEXT_PLAIN_MIME);
      resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      resp.getWriter().println("BAD REQUEST");
    }
  }

  private StringBuffer getFullUrl(HttpServletRequest req) {
    StringBuffer sb = req.getRequestURL();
    String queryString = req.getQueryString();
    if (queryString != null) {
      sb.append("?").append(queryString);
    }
    return sb;
  }
}
