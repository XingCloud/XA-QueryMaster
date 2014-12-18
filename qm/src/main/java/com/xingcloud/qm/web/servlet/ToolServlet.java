package com.xingcloud.qm.web.servlet;

import com.xingcloud.qm.service.QueryMaster;
import org.apache.log4j.Logger;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;

public class ToolServlet extends HttpServlet {

    private static final Logger LOGGER = Logger.getLogger(ToolServlet.class);

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        doPost(req, resp);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        try {
            String status = QueryMaster.getInstance().getStatus();
            PrintWriter pw = new PrintWriter(resp.getOutputStream());
            pw.write(status);
            pw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
