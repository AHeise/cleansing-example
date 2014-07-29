package eu.stratosphere.sopremo.cleansing.examples;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.meteor.QueryParser;
import eu.stratosphere.sopremo.client.DefaultClient;
import eu.stratosphere.sopremo.operator.SopremoPlan;
import eu.stratosphere.sopremo.query.QueryParserException;
import eu.stratosphere.sopremo.server.SopremoTestServer;

public class ScriptRunner{
	protected SopremoTestServer testServer;

	protected DefaultClient client;

	protected File inputDir;
	
	@Test
	public void runCongress() throws IOException {
		final SopremoPlan plan = parseScript( new File("src/main/meteor/finalScripts/"+"congress"+".script"));
		//SopremoUtil.trace();
		this.client.submit(plan, null, true);
	}	
	
	@Test
	public void runEarmarks() throws IOException {
		final SopremoPlan plan = parseScript( new File("src/main/meteor/finalScripts/"+"earmarks"+".script"));
		//SopremoUtil.trace();
		this.client.submit(plan, null, true);
	}	
	
	@Test
	public void runFreebase() throws IOException {
		final SopremoPlan plan = parseScript( new File("src/main/meteor/finalScripts/"+"freebase"+".script"));
		//SopremoUtil.trace();
		this.client.submit(plan, null, true);
	}
	
	@Test
	public void runSpendings() throws IOException {
		final SopremoPlan plan = parseScript( new File("src/main/meteor/finalScripts/"+"spendings"+".script"));
		//SopremoUtil.trace();
		this.client.submit(plan, null, true);
	}
	
	@Test
	public void runRecordLinkageAndFusionPersons() throws IOException {
		final SopremoPlan plan = parseScript( new File("src/main/meteor/finalScripts/"+"recordLinkageAndFusionPersons"+".script"));
		//SopremoUtil.trace();
		this.client.submit(plan, null, true);
	}
	
	@Test
	public void runRecordLinkageAndFusionLegalEntities() throws IOException {
		final SopremoPlan plan = parseScript( new File("src/main/meteor/finalScripts/"+"recordLinkageAndFusionLegalEntities"+".script"));
		//SopremoUtil.trace();
		this.client.submit(plan, null, true);
	}

	@Before
	public final void setup() throws Exception {
		this.testServer = new SopremoTestServer(true);
		this.inputDir = this.testServer.createDir("input");

		this.client = new DefaultClient();
		this.client.setServerAddress(this.testServer.getServerAddress());
		this.client.setUpdateTime(100);
	}

	@After
	public void teardown() throws Exception {
		this.client.close();
		this.testServer.close();
	}
	
	public SopremoPlan parseScript(final File script) {
		SopremoPlan plan = null;
		try {
			final QueryParser queryParser = new QueryParser().withInputDirectory(script.getParentFile());
			
			plan = queryParser.tryParse(this.loadScriptFromFile(script));
		} catch (final QueryParserException e) {
			final AssertionError error =
				new AssertionError(String.format("could not parse script: %s", e.getMessage()));
			error.initCause(e);
			throw error;
		}

		Assert.assertNotNull("could not parse script", plan);

		return plan;
	}
	
	private String loadScriptFromFile(final File scriptFile) {
		try {
			final BufferedReader reader = new BufferedReader(new FileReader(scriptFile));
			final StringBuilder builder = new StringBuilder();
			int ch;
			while ((ch = reader.read()) != -1)
				builder.append((char) ch);
			reader.close();
			return builder.toString();
		} catch (final IOException e) {
			throw new RuntimeException(e);
		}

	}
}
