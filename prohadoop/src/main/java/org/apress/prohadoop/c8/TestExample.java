package org.apress.prohadoop.c8;

import java.io.IOException;

import junit.framework.TestCase;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mrunit.MapDriver;
import org.junit.Before;
import org.junit.Test;

public class TestExample extends TestCase {

  private Mapper mapper;
  private MapDriver driver;

  @Before
  public void setUp() {
    mapper = new IdentityMapper();
    driver = new MapDriver(mapper);
  }

  @Test
  public void testIdentityMapper() {
    {
		try {
			driver.withInput(new Text("foo"), new Text("bar"))
			        .withOutput(new Text("foo"), new Text("bar"))
			        .runTest();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}
  }
}