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

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class Log4jLoggersServlet extends HttpServlet {
  public static final String READ_ONLY = "loggers.servlet.read.only";
  private static final String ALL_CATEGORIES = "_ALL_CATEGORIES_";

  private boolean readOnly;

  @Override
  public void init() throws ServletException {
    Boolean b = (Boolean) getServletContext().getAttribute(READ_ONLY);
    readOnly = (b != null) ? b : true;
    readOnly = false;
  }

  private List<String> getNamesAncestry(String name) {
    List<String> names = new ArrayList<String>();
    names.add(name);
    int idx = name.lastIndexOf(".");
    while (idx > -1) {
      name = name.substring(0, idx);
      names.add(name);
      idx = name.lastIndexOf(".");
    }
    return names;
  }

  /* Get all loggers and sort them in a (loggerName, levelName) map */
  @SuppressWarnings("unchecked")
  private Map<String, String> getLoggersInfo() {
    TreeMap<String, String> map = new TreeMap<String, String>();
    Enumeration<Logger> loggers = LogManager.getCurrentLoggers();
    List<String> names = new ArrayList<String>();
    while (loggers.hasMoreElements()) {
      Logger logger = loggers.nextElement();
      if (!logger.getName().equals("ROOT") && !logger.getName().isEmpty()) {
        names.addAll(getNamesAncestry(logger.getName()));
      }
    }
    for (String name : names) {
        map.put(name, LogManager.getLogger(name).getEffectiveLevel().toString());
    }
    return map;
  }

  /**
   * Present a form to let the user choose log level for different loggers.
   */
  @Override
  public void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    resp.setContentType("text/html");
    resp.setStatus(HttpServletResponse.SC_OK);
    StringBuilder sb = new StringBuilder(4096);
    sb.append("<html><head><title>Llama Loggers Config</title></head><body>");
    doLoggersTable(sb, getLoggersInfo());
    sb.append("</body></html>");
    resp.getWriter().println(sb.toString());
  }

  private void doLoggersTable(StringBuilder sb, Map<String, String> loggerMap) {
    String info = (readOnly) ? " (READ ONLY)" : "";
    sb.append("<h2>Llama Loggers Config").append(info).append("</h2>");
    sb.append("<table border='1'>");
    sb.append("<tr>");
    sb.append("<td colspan='3'><center>");
    doLevelSelector(sb, "<b>Global Level</b> ", ALL_CATEGORIES);
    sb.append("</center></td>");
    sb.append("</tr>");
    sb.append("<tr>");
    sb.append("<th>Category</th>");
    sb.append("<th>Level</th>");
    if (!readOnly) {
      sb.append("<th>Change</th>");
    }
    sb.append("</tr>");
    for (Map.Entry<String, String> entry : loggerMap.entrySet()) {
      doLoggerRow(sb, entry.getKey(), entry.getValue());
    }
    sb.append("</table>");
  }

  private static final List<String> LEVELS =
      Arrays.asList("OFF", "FATAL", "ERROR", "WARN", "INFO", "DEBUG", "TRACE");

  private void doLoggerRow(StringBuilder sb, String category, String level) {
    sb.append("<tr>");
    sb.append("<td>").append(category).append("</td>");
    sb.append("<td><b>").append(level).append("</b></td>");
    if (!readOnly) {
      sb.append("<td>");
      doLevelSelector(sb, "", category);
      sb.append("</td>");
    }
    sb.append("</tr>");
  }

  private void doLevelSelector(StringBuilder sb, String msg, String category) {
    sb.append("<form method='POST'>");
    sb.append(msg);
    sb.append("<input type='hidden' name='logger' value='").append(category).
        append("'/>");
    for (String level : LEVELS) {
      sb.append("<input type='submit' name='level' value='").append(level).
          append("'/>");
    }
    sb.append("</form>");
  }

  /**
   * Handle setting the new log level
   */
  @Override
  public void doPost(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    if (!readOnly) {
      String loggerName = req.getParameter("logger");
      if (loggerName == null || loggerName.isEmpty()) {
        throw new ServletException("Logger name is not set");
      }

      String levelName = req.getParameter("level");
      if (levelName == null || levelName.isEmpty()) {
        throw new ServletException("Level is not set");
      }

      Level level = Level.toLevel(levelName);
      if (level == null) {
        throw new ServletException("'" + levelName + "' is not valid");
      }

      if (loggerName.equals(ALL_CATEGORIES)) {
        Enumeration<Logger> loggers = LogManager.getCurrentLoggers();
        while (loggers.hasMoreElements()) {
          loggers.nextElement().setLevel(level);
        }
      } else {
        LogManager.getLogger(loggerName).setLevel(level);
      }
    }
    doGet(req, resp);
  }

}
