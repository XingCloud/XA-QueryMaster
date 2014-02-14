package com.xingcloud.qm.servlet;

import com.caucho.hessian.client.HessianProxyFactory;
import com.xingcloud.qm.exceptions.XRemoteQueryException;
import com.xingcloud.qm.service.Submit;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.drill.common.util.FileUtils;
import org.apache.http.client.utils.URIBuilder;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * User: Z J Wu Date: 13-8-13 Time: 下午6:49 Package: com.xingcluod.qm.servlet
 */
public class TestQueryMasterServlet {
  private static final HessianProxyFactory FACTORY = new HessianProxyFactory();

  @Test
  public void test() throws URISyntaxException, IOException, ClassNotFoundException, XRemoteQueryException {
    String file = "/Plans/allevent_plans/random-plan.2.json";
    File realFile= FileUtils.getResourceAsFile(file);
    StringBuilder sb;
    String line, plan;
    sb = new StringBuilder();
    try (BufferedReader br = new BufferedReader(new FileReader(realFile))) {
      while ((line = br.readLine()) != null) {
        sb.append(StringUtils.trimToEmpty(line));
      }
    }
    plan = sb.toString();

    URIBuilder builder = new URIBuilder();
    builder.setScheme("HTTP");
    builder.setHost("10.1.18.126");
    builder.setPort(8182);
    builder.setPath("/qm/q");
    URI uri = builder.build();
    Submit service = (Submit) FACTORY.create(uri.toString());
    String key = RandomStringUtils.randomAlphanumeric(5);
    key="GROUP,age,2014-01-11,2014-01-12,pay.*,TOTAL_USER,VF-ALL-0-0,EVENT_VAL";
    //key="COMMON,age,2014-01-08,2014-01-08,*.*,TOTAL_USER,VF-ALL-0-0,PERIOD";
    Submit.SubmitQueryType type = Submit.SubmitQueryType.PLAN;
    if (service.submit(key, plan, type)) {
      System.out.println("Submit ok - " + key);
    } else {
      System.out.println("Submit failed - " + key);
    }
  }
}
