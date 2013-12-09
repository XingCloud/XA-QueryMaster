package com.xingcloud.qm.web.servlet;

import com.xingcloud.qm.service.QueryMaster;
import com.xingcloud.qm.utils.PlanWriter;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.Writer;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 12/6/13
 * Time: 3:02 PM
 * To change this template use File | Settings | File Templates.
 */
public class ListTaskQueueServlet extends HttpServlet {
  public void service(HttpServletRequest request, HttpServletResponse response) throws IOException {
    StringBuilder queryIds = new StringBuilder();
    for (String queryId : QueryMaster.getInstance().submitted.keySet()) {
      queryIds.append(queryId).append("\r\n");
    }
    Writer writer =response.getWriter();
    writer.write(queryIds.toString());
    writer.flush();
  }
}
