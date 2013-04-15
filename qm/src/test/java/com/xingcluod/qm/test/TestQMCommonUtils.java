package com.xingcluod.qm.test;

import static com.xingcloud.qm.utils.QueryMasterCommonUtils.hasGroupByKeyWord;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

public class TestQMCommonUtils {

  @Before
  public void setUp() throws Exception {
  }

  @Test
  public void testHasGroupByKeyWord() {
    String s = null;
    assertFalse("Null, should return false.", hasGroupByKeyWord(s));

    s = "select * from a";
    assertFalse("Do not has group by, should return false.", hasGroupByKeyWord(s));

    s = "select * from a groupby a";
    assertFalse("Wrong group by key words, should return false.", hasGroupByKeyWord(s));

    s = "select * from a Group by a";
    assertFalse("Group by key word are not all upper case, should return false.", hasGroupByKeyWord(s));

    s = "SELECT * FROM a GROUP BY b";
    assertTrue("Correct group by", hasGroupByKeyWord(s));
  }

}
